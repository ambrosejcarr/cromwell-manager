#!/usr/bin/python
import argparse
import json
import math
import urllib2
# pip install python-dateutil
import dateutil.parser
from StringIO import StringIO
import gzip

GCE_MACHINE_TYPES_URL = "http://cloudpricingcalculator.appspot.com/static/data/pricelist.json"
TOTAL_WORKFLOW_COST = 0

# load the US pricing for both persistent disk and compute engine
def get_gce_pricing():
    response = urllib2.urlopen(GCE_MACHINE_TYPES_URL)
    data = response.read()

    if response.info().get('Content-Encoding') == 'gzip':
        buf = StringIO(data)
        f = gzip.GzipFile(fileobj=buf)
        data = f.read()

    pricing = json.loads(data)

    data = {}
    for k, v in pricing.iteritems():
        if k == "gcp_price_list":
            for k2, v2 in v.iteritems():
                if k2.startswith("CP-COMPUTEENGINE-VMIMAGE"):
                    data[k2.replace("CP-COMPUTEENGINE-VMIMAGE-", "").lower()] = v2['us']
                if k2.startswith("CP-COMPUTEENGINE-STORAGE-PD"):
                    data[k2] = v2['us']
                if k2.startswith("CP-COMPUTEENGINE-STORAGE-PD-CAPACITY"):
                    data[k2] = v2['us']

    return data


def extract_machine_type(t):
    base_machine_type = "unknown"

    if 'jes' in t and 'machineType' in t['jes']:
        full_machine = t['jes']['machineType']
        (zone, base_machine_type) = full_machine.split("/")

    return base_machine_type


def get_disk_info(metadata):
    if "runtimeAttributes" in metadata and "disks" in metadata['runtimeAttributes']:
        bootDiskSizeGb = 0.0
        if "bootDiskSizeGb" in metadata['runtimeAttributes']:
            bootDiskSizeGb = float(metadata['runtimeAttributes']['bootDiskSizeGb'])
        # Note - am lumping boot disk in with requested disk.  Assuming boot disk is same type as requested.
        # i.e. is it possible that boot disk is HDD when requested is SDD.
        (name, disk_size, disk_type) = metadata['runtimeAttributes']["disks"].split()
        return {"size": float(disk_size) + bootDiskSizeGb, "type": "PERSISTENT_" + disk_type}
    else:
        # we can't tell disk size in this case so just return nothing
        return {"size": float(0), "type": "PERSISTENT_SSD"}


def was_preemptible_vm(metadata):
    if "runtimeAttributes" in metadata and "preemptible" in metadata['runtimeAttributes']:
        pe_count = int(metadata['runtimeAttributes']["preemptible"])
        attempt = int(metadata['attempt'])

        return attempt <= pe_count
    else:
        # we can't tell (older metadata) so conservatively return false
        return False

def used_cached_results(metadata):
    return "callCaching" in metadata and metadata["callCaching"]["hit"]

def calculate_runtime(call_info, ignore_preempted):
    # get start (start time of VM start) & end time (end time of 'ok') according to metadata
    found_start = False
    found_end = False

    # give a runtime of 0 for preempted jobs so they have no cost associated with them
    if was_preempted(call_info) and ignore_preempted:
        return 0

    if 'executionEvents' in call_info:
        for x in call_info['executionEvents']:
            y = x['description']

            if y.startswith("start"):
                start = dateutil.parser.parse(x['startTime'])
                found_start = True

            if y.startswith("ok"):
                end = dateutil.parser.parse(x['endTime'])
                found_end = True

    # if we are preempted or if cromwell used previously cached results, we don't even get a start time from JES
    # use the Cromwell start time which is earlier but not wrong
    if not found_start and (was_preempted(call_info) or used_cached_results(call_info)):
        start = dateutil.parser.parse(call_info['start'])
        found_start = True

    # if we are preempted or if cromwell used previously cached results, we don't get an endtime from JES right now...
    # use the Cromwell end time which is later but not wrong
    if not found_end and (was_preempted(call_info) or used_cached_results(call_info)):
        end = dateutil.parser.parse(call_info['end'])
        found_end = True

    if not found_start or not found_end:
        exit("Unable to find start or end", call_info)

    # round up to nearest minute and also minimum billing time is 10 minutes
    run_minutes = max(10.0, math.ceil((end - start).total_seconds() / 60.0))
    run_hours = run_minutes / 60.0
    return run_hours

def was_preempted(call_info):
    # We treat Preempted and RetryableFailure the same.  The latter is a general case of the former
    return call_info['executionStatus'] in ['Preempted', 'RetryableFailure']

def calculate_cost(metadata, ignore_preempted, only_total_cost, print_header):
    # set up pricing information
    pricing = get_gce_pricing()
    ssd_cost_per_gb_per_month = float(pricing["CP-COMPUTEENGINE-STORAGE-PD-SSD"])
    ssd_cost_per_gb_hour = (ssd_cost_per_gb_per_month / (24 * 365 / 12))

    hdd_cost_per_gb_per_month = float(pricing["CP-COMPUTEENGINE-STORAGE-PD-CAPACITY"])
    hdd_cost_per_gb_hour = (hdd_cost_per_gb_per_month / (24 * 365 / 12))

    disk_costs = {"PERSISTENT_SSD": ssd_cost_per_gb_hour, "PERSISTENT_HDD": hdd_cost_per_gb_hour}

    if print_header and not only_total_cost:
        # print out a header
        print "\t".join(
            ["task_name", "status", "machine_type", "total_hours", "cpu_cost_per_hour", "cpu_cost", "pe_total_hours",
             "pe_cpu_cost_per_hour", "pe_cpu_cost", "failed_pe_total_hours", "failed_pe_cpu_cost", "disk_type",
             "disk_size", "disk_gb_hours", "disk_cost", "failed_pe_ssd_gb_hours", "failed_pe_ssd_cost", "total_cost"])

    # iterate through the metadata file for each call
    for k, v in metadata['calls'].iteritems():
        task_name = k

        total_hours = 0
        pe_total_hours = 0
        failed_pe_total_hours = 0
        machine_type = "unknown"
        complete = True

        for call_info in v:
            # this is a subworkflow, recursively calculate cost on workflow metadata
            if 'subWorkflowMetadata' in call_info:
                calculate_cost(call_info['subWorkflowMetadata'], ignore_preempted, only_total_cost, False)
            else:
                # only process things that are not in flight
                if call_info['executionStatus'] in ['Running', 'NotStarted', 'Starting']:
                    complete = False
                else:
                    if call_info['executionStatus'] in ['Failed']:
                        complete = False

                    if machine_type == "unknown":
                        machine_type = extract_machine_type(call_info)

                    pe_vm = was_preemptible_vm(call_info)
                    disk_info = get_disk_info(call_info)

                    run_hours = calculate_runtime(call_info,ignore_preempted)

                    # for preemptible VMs, separately tally successful tasks vs ones that were preempted
                    if pe_vm:
                        if was_preempted(call_info):
                            failed_pe_total_hours += run_hours
                        else:
                            pe_total_hours += run_hours
                    else:
                        total_hours += run_hours

        if complete:
            status = "complete"
        else:
            status = "incomplete"

        if machine_type == "unknown":
            cpu_cost_per_hour = 0
            pe_cpu_cost_per_hour = 0
        else:
            cpu_cost_per_hour = pricing[machine_type]
            pe_cpu_cost_per_hour = pricing[machine_type + "-preemptible"]

        cpu_cost = total_hours * cpu_cost_per_hour
        failed_pe_cpu_cost = failed_pe_total_hours * pe_cpu_cost_per_hour
        pe_cpu_cost = pe_total_hours * pe_cpu_cost_per_hour

        disk_cost_per_gb_hour = disk_costs[disk_info["type"]]

        disk_gb_hours = disk_info["size"] * (total_hours + pe_total_hours)
        disk_cost = disk_gb_hours * disk_cost_per_gb_hour

        failed_pe_disk_gb_hours = disk_info["size"] * failed_pe_total_hours
        failed_pe_disk_cost = failed_pe_disk_gb_hours * disk_cost_per_gb_hour

        total_cost = cpu_cost + pe_cpu_cost + failed_pe_cpu_cost + disk_cost + failed_pe_disk_cost

        # accumalate total workflow cost
        global TOTAL_WORKFLOW_COST
        TOTAL_WORKFLOW_COST += total_cost

        if not only_total_cost:
            out = (
                task_name, status, machine_type, total_hours, cpu_cost_per_hour, cpu_cost, pe_total_hours,
                pe_cpu_cost_per_hour, pe_cpu_cost, failed_pe_total_hours, failed_pe_cpu_cost, disk_info["type"],
                disk_info["size"], disk_gb_hours, disk_cost, failed_pe_disk_gb_hours, failed_pe_disk_cost, total_cost)
            print('\t'.join(map(str, out)))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-m", "--metadata", dest="metadata", required=True, help="metadata file to calculate cost on")
    parser.add_argument("--ignore_preempted", dest="ignore_preempted", action="store_true", help="ignore preempted tasks")
    parser.add_argument("--only_total", dest="only_total_cost", action="store_true", help="print total cost of the workflow instead of the tsv per task costs")

    args = parser.parse_args()

    with open(args.metadata) as data_file:
        metadata = json.load(data_file)
    calculate_cost(metadata, args.ignore_preempted, args.only_total_cost, True)

    if args.only_total_cost:
        print "Total Cost: " + str(TOTAL_WORKFLOW_COST)

if __name__ == "__main__":
    main()

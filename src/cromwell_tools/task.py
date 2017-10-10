from cromwell_tools.resource_utilization import ResourceUtilization
from cromwell_tools.io_util import GSObject


# todo tasks can have one or more subworkflow ids (one per shard)
# todo tasks do not currently have options for multiple resource utilizations (several shards, due
# to scatter) but they should!
class Task:
    """Object to define an instance of a workflow task."""

    def __init__(self, metadata, client):
        """
        :param dict metadata: json dictionary of metadata for this task
        :param google.cloud.storage.Client client: Authenticate google storage client
        """
        self._metadata = metadata

        for log in [shard['monitoringLog'] for shard in self._metadata]:
            gs_log = GSObject(log, client)
            fileobj = gs_log.download_to_bytes_readable()
            self._resource_utilization = ResourceUtilization.from_file(
                task_name=self._metadata[0]['labels']['wdl-task-name'],
                open_log_file_object=fileobj)

    @property
    def resource_utilization(self):
        """Task resource utilization.

        :return cromwell_tools.resource_utilization.ResourceUtilization: Resource utilization for
         this task
        """
        return self._resource_utilization

    @resource_utilization.setter
    def resource_utilization(self, value):
        if not isinstance(value, ResourceUtilization):
            raise TypeError('resource utilization must be of a ResourceUtilization class')
        if self._resource_utilization is None:
            self._resource_utilization = value
        else:  # merge with existing data
            self._resource_utilization = ResourceUtilization.merge(
                self._resource_utilization, value)

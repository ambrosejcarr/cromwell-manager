from .resource_utilization import ResourceUtilization
from .io_util import GSObject


class Shard:
    """at the moment, shard is a simple named dictionary class containing shard information"""

    def __init__(self, metadata, client):
        """

        :param dict metadata: shard metadata
        """

        self._data = metadata

        gs_log = GSObject(self._data['monitoringLog'], client)
        try:
            fileobj = gs_log.download_to_bytes_readable()
            self._resource_utilization = ResourceUtilization.from_file(
                task_name=self._data['labels']['wdl-task-name'],
                open_log_file_object=fileobj)
        except AttributeError:  # monitoringLog does not exist for this task
            self._resource_utilization = None

    def __repr__(self):
        return '<Google Compute Shard: %s>' % self._data['labels']['wdl-task-name']

    def __getitem__(self, item):
        return self._data[item]

    def __setitem__(self, key, value):
        self._data[key] = value

    def __len__(self):
        return len(self._data)

    @property
    def resource_utilization(self):
        return self._resource_utilization


class CalledTask:
    """Object to define an instance of a called workflow task."""

    def __init__(self, name, shard_metadata, client):
        """
        :param str name: name of task
        :param list shard_metadata: json dictionary of metadata for this task
        :param google.cloud.storage.Client client: Authenticate google storage client
        """
        if not isinstance(shard_metadata, list):
            raise TypeError('shard_metadata must be a list, not %s' % type(shard_metadata))
        self._name = name
        self._storage_client = client
        self._shards = [Shard(s, client) for s in shard_metadata]

    def __repr__(self):
        return "<CalledTask: %s, %d shard(s)>" % (self._name, len(self._shards))

    @property
    def is_singleton(self):
        return True if len(self._shards) == 1 else False

    @property
    def is_scattered(self):
        return not self.is_singleton

    @property
    def name(self):
        return self._name

    @property
    def resource_utilization(self):
        if self.is_singleton:
            return self._shards[0].resource_utilization
        else:
            first = self._shards[0].resource_utilization
            for next_shard in self._shards[1:]:
                first = ResourceUtilization.merge(first, next_shard.resource_utilization)
            return first

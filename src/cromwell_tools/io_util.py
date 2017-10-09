from io import BytesIO, BufferedIOBase
from google.cloud import storage
import requests


class GSObject:

    def __init__(self, gs_filestring, client=None):
        """Object for downloading google storage blobs.

        :param str gs_filestring: google storage url for file to be downloaded
        :param google.cloud.storage.Client | None client: (optional) authenticated google storage
          client
        """

        # get client
        if isinstance(client, storage.Client):
            self.client = client
        elif client is None:
            self.client = storage.Client()
        else:
            raise TypeError('client must be a google.cloud.storage.Client object or None, not %s'
                            % type(client))

        # get bucket, blob from filestring
        if isinstance(gs_filestring, str) and gs_filestring.startswith('gs://'):
            bucket, blob = self.split_path(gs_filestring)
            self.bucket = self.client.bucket(bucket)
            self.blob = self.bucket.get_blob(blob)
        else:
            raise TypeError('gs_filestring must be a string that startswith "gs://"')

    @staticmethod
    def split_path(path):
        """Utility to split a google storage path into bucket + key.

        :param str path: google storage path (must have gs:// prefix)
        :return str: bucket
        :return str: blob
        """
        if not path.startswith('gs://'):
            raise ValueError('%s path is not a valid code review')
        prefix, _, bucket, *blob = path.split('/')

        return bucket, '/'.join(blob)

    def download_as_string(self):
        """Download data as a string

        :return str: downloaded blob data
        """
        return self.blob.download_as_string().decode()

    def download_to_file(self, file_object):
        """Download data to file

        :param io.BufferedIOBase file_object: open bytes-writable file object
        """
        if not isinstance(file_object, BufferedIOBase):
            raise TypeError('file_object must be an open, writable file object')
        self.blob.download_to_file(file_object)

    def download_to_bytes_readable(self):
        """Return a bytes file-like object readable by requests and REST APIs

        :return BufferedIOBase: readable file object
        """
        string_buffer = BytesIO()
        self.blob.download_to_file(string_buffer)
        string_buffer.seek(0)
        return string_buffer


class HTTPObject:

    def __init__(self, url):
        """Object for downloading files at http or https endpoints.

        e.g. github raw endpoints

        :param str url: url of data to be downloaded to file
        """
        if isinstance(url, str) and (url.startswith('http://') or url.startswith('https://')):
            self.url = url
        else:
            raise TypeError('url must be a str that starts with http:// or https://')

    def download_as_string(self):
        """Download data as a string

        :return str: downloaded url data
        """
        return requests.get(self.url).content.decode()

    def download_to_file(self, file_object):
        """Download data to file

        :param io.BufferedIOBase file_object: open bytes-writable file object
        """
        bytestring = requests.get(self.url).content
        file_object.write(bytestring)

    def download_to_bytes_readable(self):
        """Return a bytes file-like object readable by requests and REST APIs

        :return BufferedIOBase: readable file object
        """
        bytestring = requests.get(self.url).content
        buffer = BytesIO(bytestring)
        buffer.seek(0)
        return buffer

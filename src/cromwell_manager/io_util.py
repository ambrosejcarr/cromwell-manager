import os
import sys
from io import BytesIO, BufferedIOBase
from tempfile import NamedTemporaryFile
import zipfile
import datetime
from google.cloud import storage
import webbrowser
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

    def exists(self):
        return True if requests.head(self.url).status_code == 200 else False


def package_workflow_dependencies(**dependencies):
    """Download wdls, zip, and return a bytes-readable output

    :param dependencies: dict of dependency (name, path) pairs to be included in the archive
      - name should be the expected name for the imported dependency
      - path should give the object's location, supports google storage, https, and local paths
    :return File: file object with binary data written.
    """
    archive_buffer = NamedTemporaryFile(delete=False)
    archive = zipfile.ZipFile(archive_buffer, 'a')

    for name, dependency in dependencies.items():
        if dependency.startswith('gs://'):
            dependency_data = GSObject(dependency).download_as_string()
            archive.writestr(name, dependency_data)
        if dependency.startswith('https://') or dependency.startswith('http://'):
            dependency_data = HTTPObject(dependency).download_as_string()
            archive.writestr(name, dependency_data)
        else:  # assume filepath
            archive.write(dependency, arcname=name)

    archive.close()  # writes essential records
    archive_buffer.close()  # clean up, will be deleted on program termination
    return open(archive_buffer.name, 'rb')


def open_gs_console(link, project):
    """open the google storage console to view the contents of link

    :param str link: gs file or directory
    :param str project: project owner of link
    """
    if link.startswith('gs://'):
        link = link.replace('gs://', '')
        link = 'https://storage.cloud.google.com/{link}'.format(link=link)
    if link.endswith('/'):
        link += '?project={project}'.format(project=project)
        link = 'https://console.cloud.google.com/storage/browser/{link}'.format(link=link)

    webbrowser.open(link)


def check_exists(file_or_link):
    """check that a file or link points to a valid location

    :param str file_or_link:
    :return bool:
    """
    if file_or_link.startswith('http'):
        rc = requests.head(file_or_link).status_code
        if rc == 200:
            announce('checking {}... OK.'.format(file_or_link))
        else:
            announce('checking {}... returned code {!s}, FAIL.'.format(file_or_link, rc))
    elif file_or_link.startswith('gs://'):
        if GSObject(file_or_link).blob.exists():
            announce('checking {}... OK.'.format(file_or_link))
        else:
            announce('checking {}... does not exist!, FAIL.'.format(file_or_link))
    else:
        if os.path.isfile(file_or_link):
            announce('checking {}... OK.')
        else:
            announce('checking {}... not a valid file, FAIL.'.format(file_or_link))


def announce(message):
    print('CWM:{}:{}'.format(datetime.datetime.now(), message))

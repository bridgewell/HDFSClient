import logging
import os
import shutil
import json
from os import path as os_path
from os.path import join as path_join
from uuid import uuid1
from time import time
from collections import namedtuple, deque

import requests
from requests import RequestException
from requests.compat import urljoin

from .exceptions import ActiveNamenodeNotFoundException
from .request_wrapper import RetryAction, ResultValue

logger = logging.getLogger(__name__)

NameNode = namedtuple('NameNode', ['host', 'port'])
HDFSData = namedtuple('HDFSData', ['dirs', 'files'])


class QueryURL(object):
    NAMENODE_STATUS = 'http://{host}:{port}/jmx?qry=Hadoop:service=NameNode,name=NameNodeStatus'
    WEBHDFS = 'http://{{host}}:{{port}}/webhdfs/v1/{0}'
    MAKEDIRS = '?op=MKDIRS'
    DELETE = '?op=DELETE&recursive={0}'
    RENAME = '?op=RENAME&destination={0}'
    CREATE = '?op=CREATE&overwrite={0}'
    LISTDIR = '?op=LISTSTATUS'
    OPEN = '?op=OPEN'
    STATUS = '?op=GETFILESTATUS'


class DataType(object):
    FILE = 'FILE'
    DIRECTORY = 'DIRECTORY'


def retry_request(exceptions, default_return_value=ResultValue.NOTHING):

    def outer_wrapper(func):

        def func_wrapper(ins, *args, **kwargs):
            proc = RetryAction(func, exceptions, default_return_value, error_trigger=ins.request_error_trigger)
            return proc(ins, *args, **kwargs)
        return func_wrapper

    return outer_wrapper


class HDFSClient(object):

    def __init__(self, namenodes):
        """Create an instance of HDFSClient for use

        Args:
          namenodes (iterable of `NameNode`): a sequence of NameNode objects.

        """
        if len(namenodes) == 0 or len(namenodes) > 2:
            raise ValueError('length of namenodes should in range(1, 2)')

        self._nns = namenodes
        self._refresh_active_namenode()

    def request_error_trigger(self, err, is_final):
        if is_final:
            logger.error('Can\'t send request, error message:\n{}'.format(str(err)))
        else:
            logger.warn('Can\'t send request, error message:\n{}, try to refresh \
                         active HDFS NameNode and send request again'.format(str(err)))
            self._refresh_active_namenode()

    @retry_request((RequestException, ActiveNamenodeNotFoundException))
    def _refresh_active_namenode(self):

        active_index = None

        for index, nn in enumerate(self._nns):
            url = QueryURL.NAMENODE_STATUS.format(host=nn.host, port=nn.port)
            response = requests.get(url)
            if response.ok and u'active' in response.content:
                active_index = index
                break

        if active_index is None:
            raise  ActiveNamenodeNotFoundException('Can\'t find active namenode')

        if active_index == 1:
            self._nns.reverse()

        self._active_namenode = self._nns[0]
        self._last_refresh_ts = time()

    def _format_namenode(self, url):
        return url.format(host=self._active_namenode.host,
                          port=self._active_namenode.port)

    def _send_request(self, url, request_method, **kwargs):
        request_url = self._format_namenode(url)
        response = request_method(request_url, **kwargs)
        return response

    def _send_simple_request(self, url, request_method):
        response = self._send_request(url, request_method)
        is_success = response.ok
        response.close()
        return is_success

    @retry_request(RequestException, False)
    def makedirs(self, path):
        url = urljoin(QueryURL.WEBHDFS.format(path.lstrip('/')),
                      QueryURL.MAKEDIRS)
        return self._send_simple_request(url, requests.put)

    @retry_request(RequestException, False)
    def rename(self, src, dst):
        url = urljoin(QueryURL.WEBHDFS.format(src.lstrip('/')),
                      QueryURL.RENAME.format(dst))
        return self._send_simple_request(url, requests.put)

    @retry_request(RequestException, False)
    def delete(self, path, recursive=False):
        url = urljoin(QueryURL.WEBHDFS.format(path.lstrip('/')),
                      QueryURL.DELETE.format(recursive))
        return self._send_simple_request(url, requests.delete)

    @retry_request(RequestException, None)
    def listdir(self, path):
        url = urljoin(QueryURL.WEBHDFS.format(path.lstrip('/')),
                      QueryURL.LISTDIR)

        response = self._send_request(url, requests.get)
        if response.ok:
            data_list = json.loads(response.content)[u'FileStatuses'][u'FileStatus']
            response.close()
            dirs = tuple(d[u'pathSuffix'] for d in data_list if d[u'type'] == DataType.DIRECTORY and d[u'pathSuffix'])
            files = tuple(f[u'pathSuffix'] for f in data_list if f[u'type'] == DataType.FILE and f[u'pathSuffix'])
            hdfs_data = HDFSData(dirs=dirs, files=files)
            return hdfs_data
        else:
            response.close()
            return None

    def _get_remote_data_type(self, path):
        url = urljoin(QueryURL.WEBHDFS.format(path.lstrip('/')),
                      QueryURL.STATUS)

        response = self._send_request(url, requests.get)
        if response.ok:
            status = json.loads(response.content)[u'FileStatus']
            response.close()
            return status[u'type']
        else:
            response.close()
            return None

    def _get_local_data_type(self, path):
        if os_path.exists(path):
            if os_path.isdir(path):
                return DataType.DIRECTORY
            elif os_path.isfile(path):
                return DataType.FILE
        return None

    def upload(self, local_path, remote_path, overwrite=False):
        if not overwrite and self._get_remote_data_type(remote_path) is not None:
            logger.error('Upload target path already exists: {}'.format(remote_path))
            return False

        data_type = self._get_local_data_type(local_path)
        if data_type is None:
            logger.error('Can\'t identify local data: {}'.format(local_path))
            return False
        else:
            return self._upload_by_tmp_data(local_path, remote_path, data_type)

    def _upload_by_tmp_data(self, local_path, remote_path, data_type):
        remote_tmp_path = self._generate_tmp_path(remote_path)

        is_success = self._upload_data(local_path, remote_tmp_path, data_type)

        if is_success:
            self._check_and_remove_remote_data(remote_path)
            self.rename(remote_tmp_path, remote_path)
        else:
            self._check_and_remove_remote_data(remote_tmp_path)

        return is_success

    def _upload_data(self, local_path, remote_path, data_type):
        if data_type == DataType.DIRECTORY:
            return self._upload_directory(local_path, remote_path)
        elif data_type == DataType.FILE:
            return self._upload_file(local_path, remote_path)
        else:
            logger.warn('Given type "{}" is not in option'.format(data_type))
            return False

    def _upload_directory(self, local_path, remote_path):
        is_success = True
        prefix_length = len(local_path) + 1
        self.makedirs(remote_path)

        for local_root, dirs, files in os.walk(local_path):
            remote_root = path_join(remote_path, local_root[prefix_length:])
            for d in dirs:
                is_success &= self.makedirs(path_join(remote_root, d))
            for f in files:
                is_success &= self._upload_file(path_join(local_root, f), path_join(remote_root, f))
        return is_success

    @retry_request(RequestException, False)
    def _upload_file(self, local_path, remote_path):
        is_success = False
        url = urljoin(QueryURL.WEBHDFS.format(remote_path.lstrip('/')),
                      QueryURL.CREATE.format(True))

        with open(local_path, 'rb') as f:
            file_name = os_path.basename(remote_path)
            response = self._send_request(url, requests.put, files={file_name: f})
            is_success = response.ok
            response.close()

        return is_success

    def download(self, remote_path, local_path, overwrite=False):
        if not overwrite and os_path.exists(local_path):
            logger.error('Download target path already exists: {}'.format(local_path))
            return False

        data_type = self._get_remote_data_type(remote_path)
        if data_type is None:
            logger.error('Can\'t query HDFS file type, path: {}'.format(remote_path))
            return False
        else:
            return self._download_by_tmp_data(remote_path, local_path, data_type)

    def _download_by_tmp_data(self, remote_path, local_path, data_type):
        local_tmp_path = self._generate_tmp_path(local_path)

        is_success = self._download_data(remote_path, local_tmp_path, data_type)

        if is_success:
            self._check_and_remove_local_data(local_path)
            os.rename(local_tmp_path, local_path)
        else:
            self._check_and_remove_data(local_tmp_path)

        return is_success

    def _download_data(self, remote_path, local_path, data_type):
        if data_type == DataType.DIRECTORY:
            return self._download_directory(remote_path, local_path)
        elif data_type == DataType.FILE:
            return self._download_file(remote_path, local_path)
        else:
            logger.warn('Given type "{}" is not in option'.format(data_type))
            return False

    def _download_directory(self, remote_path, local_path):
        is_success = True
        dir_queue = deque([''])
        while len(dir_queue) and is_success:
            current_dir = dir_queue.popleft()
            remote_dir_path = path_join(remote_path, current_dir)
            local_dir_path = path_join(local_path, current_dir)
            os.makedirs(local_dir_path)

            data_status = self.listdir(remote_dir_path)
            for d in data_status.dirs:
                dir_queue.append(path_join(current_dir, d))
            for f in data_status.files:
                is_success &= self._download_file(path_join(remote_dir_path, f),
                                                  path_join(local_dir_path, f))
        return is_success

    @retry_request((RequestException, IOError), False)
    def _download_file(self, remote_path, local_path):
        url = urljoin(QueryURL.WEBHDFS.format(remote_path.lstrip('/')),
                      QueryURL.OPEN)

        response = self._send_request(url, requests.get)
        is_success = response.ok
        if is_success:
            self._write_content(response, local_path)

        response.close()
        return is_success

    def _write_content(self, response, path):
        with open(path, 'wb') as f:
            for chunk in response:
                f.write(chunk)

    def _check_and_remove_local_data(self, path):
        if os_path.exists(path):
            if os_path.isfile(path):
                os.remove(path)
            elif os_path.isdir(path):
                shutil.rmtree(path)

    def _check_and_remove_remote_data(self, path):
        if self._get_remote_data_type(path) is not None:
            self.delete(path, True)

    def _generate_tmp_path(self, path):
        uuid = str(uuid1()).replace('-', '')
        return '{path}_{uuid}_tmp'.format(path=path, uuid=uuid)

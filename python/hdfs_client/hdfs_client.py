import logging
import os
import shutil
import json
from uuid import uuid1
from time import time
from collections import namedtuple

import requests
from requests import RequestException
from requests.compat import urljoin

from .exceptions import ActiveNamenodeNotFoundException

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


class DataType(object):
    FILE = 'FILE'
    DIRECTORY = 'DIRECTORY'


class HDFSClient(object):

    DEFAULT_ERROR_BOUNDARY = 3

    def __init__(self, namenodes, refresh_interval=60):
        """Create an instance of HDFSClient for use

        Args:
          namenodes (iterable of `NameNode`): a sequence of NameNode objects.
          refresh_interval (int): interval in seconds for refresh active namnode

        """
        if len(namenodes) == 0 or len(namenodes) > 2:
            raise ValueError('length of namenodes should in range(1, 2)')

        self._nns = namenodes
        self._refresh_active_namenode()
        self._refresh_interval = refresh_interval

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

    def _check_for_refresh_active_namenode(self):
        if time() - self._last_refresh_ts > self._refresh_interval:
            self._refresh_active_namenode()

    def _send_request(self, url, request_method):

        self._check_for_refresh_active_namenode()

        request_url = url.format(host=self._active_namenode.host,
                                 port=self._active_namenode.port)
        response = request_method(request_url)
        is_success = response.ok
        response.close()
        return is_success

    def _try_send_request(self, url, request_method, err_boundary=DEFAULT_ERROR_BOUNDARY):

        err_count = 0

        while err_count < err_boundary:
            try:
                if self._send_request(url, request_method):
                    return True
                else:
                    raise RequestException()
            except RequestException as err:
                err_count += 1
                if err_count < err_boundary:
                    logger.warn('Can\'t send request: {0} with message {1}, try to refresh \
                                active HDFS NameNode and send request again'.format(
                                    url.format(host=self._active_namenode.host, port=self._active_namenode.port),
                                    str(err)))
                    self._refresh_active_namenode()
                else:
                    logger.error('Can\'t send request: {0} with error message {1}'.format(
                                     url.format(host=self._active_namenode.host, port=self._active_namenode.port),
                                     str(err)))
        return False

    def makedirs(self, path):
        url = urljoin(QueryURL.WEBHDFS.format(path.lstrip('/')),
                      QueryURL.MAKEDIRS)
        return self._try_send_request(url, requests.put)

    def rename(self, src, dst):
        url = urljoin(QueryURL.WEBHDFS.format(src.lstrip('/')),
                      QueryURL.RENAME.format(dst))
        return self._try_send_request(url, requests.put)

    def delete(self, path, recursive=False):
        url = urljoin(QueryURL.WEBHDFS.format(path.lstrip('/')),
                      QueryURL.DELETE.format(recursive))
        return self._try_send_request(url, requests.delete)

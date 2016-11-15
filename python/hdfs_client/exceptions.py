
class HDFSClientException(Exception):
    """Base class for all HDFSClient exception"""
    pass


class ActiveNamenodeNotFoundException(HDFSClientException):

    def __init__(self, msg):
        super(ActiveNamenodeNotFoundException, self).__init__(msg)

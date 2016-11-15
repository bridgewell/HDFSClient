DEFAULT_ERROR_BOUNDARY = 3


class ResultValue(object):
    NOTHING = 1


def empty_trigger(url, ex, is_final):
    pass


class RetryAction(object):
    def __init__(self, func, exceptions, default_return_value, error_boundary=DEFAULT_ERROR_BOUNDARY, error_trigger=empty_trigger):
        self._func = func
        self._exceptions = exceptions
        self._default_return_value = default_return_value
        self._error_boundary = error_boundary
        self._error_trigger = error_trigger
        self._error_count = 0

    def __call__(self, *args, **kwargs):
        while not self._is_error_count_exceed_boundary():
            try:
                result = self._func(*args, **kwargs)
                if result == self._default_return_value:
                    error_message = ('Result return by function "{func}" is the default value, '.format(func=self._func.__name__) +
                                     'may be encounter some problem while execute')
                    self._error_handle(error_message)
                else:
                    return result
            except self._exceptions as err:
                self._error_handle(err)
                if (self._is_error_count_exceed_boundary() and 
                        self._default_return_value is ResultValue.NOTHING):
                    raise
        return self._default_return_value

    def _is_error_count_exceed_boundary(self):
        return self._error_count >= self._error_boundary

    def _error_handle(self, error_info):
        self._error_count += 1
        is_final = self._is_error_count_exceed_boundary()
        self._error_trigger(str(error_info), is_final)

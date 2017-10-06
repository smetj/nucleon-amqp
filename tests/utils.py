from gevent import Timeout
from functools import wraps, partial


class TestTimeout(Exception):
    """A test timed out."""


def _with_timeout(seconds=5, func=None):
    """Return a function that calls func, but raises an error if it does not
    return withing `seconds` seconds.
    """
    @wraps(func)
    def timeout_wrapper(*args, **kwargs):
        t = Timeout(seconds,
            TestTimeout('Timed out after %d seconds' % seconds)
        )
        t.start()
        try:
            ret = func(*args, **kwargs)
        finally:
            t.cancel()
        return ret
    return timeout_wrapper


def with_timeout(func_or_seconds):
    """A decorator to automatically fail a test after `seconds` seconds.

    This decorator is overloaded; it can be called either directly::

        @with_timeout

    or passing a number of seconds::

        @with_timeout(5)

    """
    if callable(func_or_seconds):
        return _with_timeout(func=func_or_seconds)
    else:
        seconds = func_or_seconds
        return partial(_with_timeout, seconds)


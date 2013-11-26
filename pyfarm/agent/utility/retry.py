# No shebang line, this module is meant to be imported
#
# Copyright 2013 Oliver Palmer
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


"""
Retry
=====

Functions and classes used for retrying callable objects or
deferreds.
"""

import logging
from functools import partial
from datetime import datetime, timedelta

from twisted.internet import defer, reactor
from twisted.python import log


class RetryDeferred(object):
    """
    Class which can retry a callable object which produces a deferred.  Errors
    are stored then emitted via the deferred object as a list.  This is meant
    to provide a base class when can then be wrapped or used with
    :func:`.retry_deferred`

    :param function:
        the callable object which produces a deferred object and should
        be retried

    :param success:
        the function to call on success, this value may be None

    :param failure:
        the function to pass a list of failures to, this value may be None

    :param int max_retries:
        the max number of retries the class is allowed to attempt

    :type retry_delay: int or float
    :param retry_delay:
        Number of seconds to wait between each retry.  This behavior could
        be overridden by :meth:`retry_delay`

    :type retry_delay: int or float
    :param timeout:
        after the first call has been made wait this may seconds before
        stopping further attempts to retry

    .. note::
        instancing this class does not start processing the deferreds, that
        work is done inside :meth:`__call__`

    """
    DEFAULT_RETRIES = 10
    DEFAULT_RETRY_DELAY = 1
    DEFAULT_TIMEOUT = 120

    def __init__(self, function, success, failure, headers=None,
                 max_retries=DEFAULT_RETRIES, retry_delay=DEFAULT_RETRY_DELAY,
                 timeout=DEFAULT_TIMEOUT):
        assert callable(function), "`function` must be callable"

        # timeout set?  Make sure it's a delta and that it's in the future
        if timeout is not None:
            assert isinstance(timeout, (int, float)), \
                "expected number for `timeout`"
            assert timeout > 0, "`timeout` must be > 0"
            self.timeout = timedelta(seconds=timeout)
        else:
            self.timeout = None

        # headers set?  Make sure they're of the correct type
        if headers is not None:
            assert isinstance(headers, dict), "`headers` must be a dictionary"

        self.log = partial(log.msg, system=self.__class__.__name__)
        self.runs = 0
        self.called = False
        self.success = success
        self.failure = failure
        self.headers = headers
        self.time_started = None
        self.max_retries = max_retries
        self._retry_delay = retry_delay
        self.errors = []  # updated in error
        self.deferred = defer.Deferred()

        # setup in __call__
        self._func = function
        self.func = None
        self.func_repr = None

    @property
    def failures(self):
        """property which returns a count of the number of errors"""
        return len(self.errors)

    def retry_delay(self):
        """
        Returns the length of time to delay between retries.  By
        default this uses what ever the retry was that was provided to
        :meth:`__init__` however it can be overridden at the class level so
        things like backoff could be implemented
        """
        return self._retry_delay

    def exceeded_timeout(self):
        """
        returns True if the timeout has been exceed based
        :return:
        """
        if self.time_started is None or self.timeout is None:
            return False

        return (datetime.now() - self.time_started) > self.timeout

    def exceeded_max_retries(self):
        """
        returns True if the instance has exceeded the max number
        of retries
        """
        return self.max_retries is not None and \
            len(self.errors) > self.max_retries

    def should_run(self):
        """
        Determines if the function should be run again.  If a timeout has
        been set then this method will first check to see if a timeout
        has elapsed.  If a timeout has not been set this method simply checks
        the number of errors versus the max number of retries.
        """
        if self.exceeded_timeout():
            self.log("%s has timed out" % self.func_repr)
            return False

        if self.exceeded_max_retries():
            self.log("%s exceeded the max number of retries" % self.func_repr)
            return False

        return True

    def execute_function(self):
        """
        Executes the underlying function we may be expected to retry and
        attaches to its deferred object
        """
        self.runs += 1

        if self.time_started is None:
            self.time_started = datetime.now()

        d = self.func()
        d.addCallbacks(self.deferred.callback, self.error)
        return d

    def run(self):
        """
        Called every time we should run the underlying function.  Unlike
        :meth:`execute_function` however this will insert a delay if this
        is a retry.
        """
        # TODO: add statsd log for # of runs/retries
        # TODO: stat log should include some info on type (webrequest/call/etc)
        # if we've already run at least once, then we should
        # retry using a delay
        if self.runs > 0:
            return reactor.callLater(self.retry_delay(), self.execute_function)
        else:
            return self.execute_function()

    def error(self, error):
        """
        Called whenever the internal function produces an error.  Depending
        on :meth:`should_run`'s value, this may either recall :meth:`run`
        or fail using deferred errback
        """
        # TODO: add statsd log for # of errors
        # TODO: stat log should include some info on type (webrequest/call/etc)
        if self.max_retries is not None:
            self.errors.append(error)

        if self.should_run():
            self.log("%s attempt %s failed: %s" % (
                self.func_repr, self.failures, error),
                level=logging.WARNING)

            # rerun the function
            self.run()

        else:
            self.log("%s has failed after %s attempts" % (
                self.func_repr, self.failures),
                level=logging.ERROR)

            # fire off errback to end the chain
            self.deferred.errback(self.errors)

    def __call__(self, *args, **kwargs):
        """
        Calls the function provided to :meth:`__init__` and passes in ``*args``
        and ``**kwargs``.  After performing some internal setup this
        function calls :meth:`run`
        """
        assert self.called is False, "__call__ already run for this instance"
        self.called = True

        # if headers were provided, be sure to pass them along
        # to kwargs before the function is called
        if self.headers is not None:
            headers = kwargs.setdefault("headers", {})
            for key, value in self.headers.iteritems():
                if key not in headers:
                    headers[key] = value

        # prepare some attribute to reuse the call the function
        self.func = lambda: self._func(*args, **kwargs)
        self.func_repr = "%s(*%s, **%s)" % (self._func.__name__, args, kwargs)
        self.run()

        # we might have been setup without callbacks (which is why
        # we're returning the deferred)
        if self.success is not None and self.failure is not None:
            self.deferred.addCallbacks(self.success, self.failure)

        self.time_started = datetime.now()
        return self.deferred


def retry_deferred(func, *args, **kwargs):
    """
    Nearly identical to :class:`.RetryCallable` except it handles the
    instancing of the class internally and return the deferred it produces.

    .. note::
        ``max_retries``, ``retry_delay``, and `timeout`` are popped
        from the ``kwargs`` dictionary to feed into :class:`.RetryCallable`
    """
    # setup the instance
    retry_callable = RetryDeferred(
        func, None, None,
        max_retries=kwargs.pop(
            "max_retries", RetryDeferred.DEFAULT_RETRIES),
        retry_delay=kwargs.pop(
            "retry_delay", RetryDeferred.DEFAULT_RETRY_DELAY),
        timeout=kwargs.pop(
            "timeout", RetryDeferred.DEFAULT_TIMEOUT))

    # run the instance and return the deferred object
    return retry_callable(*args, **kwargs)

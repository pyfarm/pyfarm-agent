
from twisted.internet import reactor

from pyfarm.agent.http.client import get


def success(data, response, request):
    print data


def failure(response):
    print response.getTraceback()

deferred = get(
    "http://127.0.0.1:5000/foo",
    response_success=success, request_failure=failure)

reactor.run()

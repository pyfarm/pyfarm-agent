from twisted.internet import reactor
from pyfarm.agent.http.client import get

def success(response):
    return response.request.retry()

deferred = get(
    "http://httpbin.org/get",
    callback=success)

reactor.run()

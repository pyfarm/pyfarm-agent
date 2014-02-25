from twisted.internet import reactor
from pyfarm.agent.http.client import get


def success(response):
    pass

deferred = get(
    "http://127.0.0.1:5000",
    callback=success)

reactor.run()

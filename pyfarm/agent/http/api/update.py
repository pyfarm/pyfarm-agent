# No shebang line, this module is meant to be imported
#
# Copyright 2014 Oliver Palmer
# Copyright 2014 Ambient Entertainment GmbH & Co. KG
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

try:
    from httplib import OK, ACCEPTED
except ImportError:  # pragma: no cover
    from http.client import OK, ACCEPTED

from twisted.internet import reactor
from twisted.web.server import NOT_DONE_YET
from treq import content

from pyfarm.agent.http.api.base import APIResource
from pyfarm.agent.http.core.client import get, post, http_retry_delay
from pyfarm.agent.config import config
from pyfarm.agent.logger import getLogger

logger = getLogger("agent.http.update")


class Update(APIResource):
    isLeaf = False  # this is not really a collection of things

    def post(self, **kwargs):
        request = kwargs["request"]
        data = kwargs["data"]
        agent = config["agent"]

        # TODO Check version parameter for sanity

        url = "%s/agents/updates/%s" % (config["master-api"], data["version"])

        def download_update(version):
            get(url,
                callback=update_downloaded,
                errback=lambda: reactor.callLater(http_retry_delay(),
                                                  download_update))

        def update_downloaded(response):
            if response.code == OK:
                with open(join(config["updates_drop_dir"],
                               "pyfarm-agent.zip")) as update_file:
                    update_file.write(content(request))
                config["restart_requested"] = True
                if len(config["current_assignments"]) == 0:
                    agent.stop()
            else:
                pass # TODO

        download_update(data["version"])

        request.setResponseCode(ACCEPTED)
        request.finish()

        return NOT_DONE_YET

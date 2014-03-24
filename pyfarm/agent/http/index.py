# No shebang line, this module is meant to be imported
#
# Copyright 2014 Oliver Palmer
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

from twisted.web.server import NOT_DONE_YET

from pyfarm.core.enums import AgentState
from pyfarm.core.sysinfo import cpu, memory
from pyfarm.agent.config import config
from pyfarm.agent.http.resource import Resource


class Index(Resource):
    """serves request for the root, '/', target"""
    TEMPLATE = "index.html"

    def get(self, request):
        # write out the results from the template back
        # to the original request
        def cb(content):
            request.write(content)
            request.setResponseCode(200)
            request.finish()

        # convert the state integer to a string
        for key, value in AgentState._asdict().iteritems():
            if config["state"] == value:
                state = key.title()
                break
        else:
            raise KeyError("failed to find state")

        # data which will appear in the table
        table_data = [
            ("Master API", config["master-api"]),
            ("Agent API", config["agent"].agent_api()),
            ("State", state),
            ("Hostname", config["hostname"]),
            ("Agent ID", config.get("agent-id", "UNASSIGNED")),
            ("IP Address", config["ip"]),
            ("Port", config["port"]),
            ("Reported CPUs", config["cpus"]),
            ("Reported RAM", config["ram"]),
            ("Total CPUs", cpu.total_cpus()),
            ("Total RAM", int(memory.total_ram())),
            ("Free RAM", int(memory.ram_free())),
            ("Total Swap", int(memory.total_swap())),
            ("Free Swap", int(memory.swap_free()))]

        # schedule a deferred so the reactor can get control back
        deferred = self.template.render(table_data=table_data)
        deferred.addCallback(cb)

        return NOT_DONE_YET

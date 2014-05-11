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

import time
from datetime import timedelta
from httplib import OK

import psutil
from twisted.web.server import NOT_DONE_YET
from pyfarm.core.utility import convert
from pyfarm.core.enums import AgentState
from pyfarm.agent.config import config
from pyfarm.agent.http.core.resource import Resource
from pyfarm.agent.sysinfo import cpu, memory, network


def mb(value):
    if isinstance(value, float):
        value = int(value)
    return str(value) + "MB"


def seconds(value):
    if value is not None:
        return "%.2f seconds" % value


class Index(Resource):
    """serves request for the root, '/', target"""
    TEMPLATE = "index.html"

    def get(self, **kwargs):
        request = kwargs["request"]

        # write out the results from the template back
        # to the original request
        def cb(content):
            request.write(content)
            request.setResponseCode(OK)
            request.finish()

        # convert the state integer to a string
        for key, value in AgentState._asdict().iteritems():
            if config["state"] == value:
                state = key.title()
                break
        else:
            raise KeyError("failed to find state")

        # Memory usage of the agent's process
        process = psutil.Process()
        process_memory = convert.bytetomb(process.get_memory_info().rss)

        total_swap = memory.total_swap()
        ram_allocated = (memory.ram_used() / float(config["ram"])) * 100
        swap_allocated = (memory.swap_used() / total_swap) * 100

        if ram_allocated >= 100:
            ram_css = "danger"
        elif ram_allocated >= 75:
            ram_css = "warning"
        elif ram_allocated >= 50:
            ram_css = "info"
        else:
            ram_css = None

        if swap_allocated >= 20:
            swap_css = "danger"
        elif swap_allocated >= 10:
            swap_css = "warning"
        elif swap_allocated >= 5:
            swap_css = "info"
        else:
            swap_css = None

        memory_info = [
            ("RAM Used",
                "%.2f%% (%s of %s)" % (
                    ram_allocated,
                    int(memory.ram_used()), mb((config["ram"]))), ram_css),
            ("SWAP Used",
                "%.2f%% (%s of %s)" % (
                    swap_allocated,
                    int(memory.swap_used()), mb((total_swap))), swap_css),
            ("System RAM", mb(memory.total_ram()), None),
            ("System RAM (reported)", mb(config["ram"]), None),
            ("Agent RAM Usage", mb(process_memory), None)]

        network_info = [
            ("Hostname", config["hostname"]),
            ("IP Address", network.ip()),
            ("IP Address (reported)", config["ip"]),
            ("Agent Port", config["port"]),
            ("Master API", config["master-api"])]

        cpu_info = [
            ("CPUs", cpu.total_cpus()),
            ("CPUs (reported)", config["cpus"]),
            ("System Time", seconds(cpu.system_time())),
            ("User Time", seconds(cpu.user_time())),
            ("Idle Time", seconds(cpu.idle_time())),
            ("IO Wait", seconds(cpu.iowait()) or "Not Supported")]

        miscellaneous = [
            ("Database ID", config["agent-id"]),
            ("Logged On User(s)",
             ", ".join(sorted(set(user.name for user in psutil.get_users())))),
            ("Host Uptime",
             str(timedelta(seconds=time.time() - psutil.get_boot_time()))),
            ("Agent Uptime",
             str(timedelta(seconds=time.time() - config["start"])))]

        deferred = self.template.render(
            memory_info=memory_info,
            network_info=network_info,
            cpu_info=cpu_info,
            miscellaneous=miscellaneous)
        deferred.addCallback(cb)

        return NOT_DONE_YET


# TODO: form submission
# TODO: make 'port' field editable (requires restart)
# TODO: add callbacks for any field that needs to update the db
class Configuration(Resource):
    TEMPLATE = "configuration.html"

    # fields which nobody can see
    HIDDEN_FIELDS = (
        "agent", "api-endpoint-prefix", "pretty-json")

    # fields that a user can edit
    EDITABLE_FIELDS = (
        "cpus", "hostname", "http-max-retries", "http-retry-delays",
        "ip", "master-api", "memory-check-interval", "ram", "ram-report-delta",
        "time-offset", "use-address", "state", "swap-report-delta",
        "http-retry-delay")

    def get(self, **kwargs):
        request = kwargs["request"]

        # write out the results from the template back
        # to the original request
        def cb(content):
            request.write(content)
            request.setResponseCode(OK)
            request.finish()

        editable_fields = []
        non_editable_fields = []
        for key, value in sorted(config.items()):
            if key in self.HIDDEN_FIELDS:
                pass
            elif key in self.EDITABLE_FIELDS:
                editable_fields.append((key, value))
            else:
                non_editable_fields.append((key, value))

        deferred = self.template.render(
            non_editable_fields=non_editable_fields,
            editable_fields=editable_fields)
        deferred.addCallback(cb)

        return NOT_DONE_YET

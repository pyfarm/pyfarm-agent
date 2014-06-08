# No shebang line, this module is meant to be imported
#
# Copyright 2014 Oliver Palmer
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


from pyfarm.agent.config import config
from pyfarm.agent.http.api.base import APIResource
from pyfarm.agent.sysinfo import memory
from pyfarm.agent.utility import dumps


class Stop(APIResource):
    isLeaf = False  # this is not really a collection of things


class Status(APIResource):
    isLeaf = False  # this is not really a collection of things

    def get(self, **kwargs):
        return dumps(
            {"state": config["state"],
             "free_ram": int(memory.ram_free()),
             "agent"
             "jobs": list(config["jobtypes"].keys())})
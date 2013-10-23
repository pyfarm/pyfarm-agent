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


from zope.interface import implementer
from twisted.python import usage
from twisted.plugin import IPlugin
from twisted.application.service import IServiceMaker

from pyfarm.agent.sysinfo.service import SysInfoService


class Options(usage.Options):
    optParameters = [
        ("perspective-broker", "b", "127.0.0.1:50001",
         "the host:port combination which is running the perspective broker")]


@implementer(IServiceMaker, IPlugin)
class SysInfoServiceMaker(object):
    options = Options
    tapname = "pyfarm.agent.sysinfo"
    description = ("Sends and receives information from other services on "
                  "the system as well as from the master.  By default this "
                  "service does nothing and must be provided information "
                  "from external processes such as pyfarm.agent.sysinfo")

    def makeService(self, options):
        return SysInfoService(options)


serviceMaker = SysInfoServiceMaker()
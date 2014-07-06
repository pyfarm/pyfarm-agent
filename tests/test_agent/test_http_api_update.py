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

try:
    from httplib import ACCEPTED, OK, BAD_REQUEST
except ImportError:  # pragma: no cover
    from http.client import ACCEPTED, OK, BAD_REQUEST


from pyfarm.agent.http.api.update import Update
from pyfarm.agent.testutil import BaseAPITestCase


class TestUpdate(BaseAPITestCase):
    URI = "/updates"
    CLASS = Update

    # TODO: finish tests (needs to create files on master too)


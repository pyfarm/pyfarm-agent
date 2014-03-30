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

"""
Utilities
---------

Top level utilities for the agent to use internally.  Many of these
are copied over from the master (which we can't import here).
"""

from decimal import Decimal
from datetime import datetime
from json import dumps as _dumps
from UserDict import UserDict

from pyfarm.agent.config import config


def default_json_encoder(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    elif isinstance(obj, datetime):
        return obj.isoformat()


def dumps(*args, **kwargs):
    """
    Agent's implementation of :func:`json.dumps` or
    :func:`pyfarm.master.utility.jsonify`
    """
    indent = None
    pretty = config.get("pretty-json", False)
    if pretty:
        indent = 2

    if len(args) == 1 and not isinstance(args[0], (dict, UserDict)):
        obj = args[0]
    else:
        obj = dict(*args, **kwargs)

    return _dumps(obj, default=default_json_encoder, indent=indent)

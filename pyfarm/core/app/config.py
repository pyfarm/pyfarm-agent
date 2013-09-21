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

"""
Config
======

Base configuration classes for the Flask application
"""

import os
from uuid import uuid4
from warnings import warn
from pyfarm.core.warning import EnvironmentWarning



def get_secret_key(warning=True):
    """
    Gets the secret key to use.  This is done outside the class so we can
    suppress the warning
    """
    if "PYFARM_SECRET_KEY" in os.environ:
        return os.environ["PYFARM_SECRET_KEY"]
    elif warning:
        warn("$PYFARM_SECRET_KEY not present in environment",
             EnvironmentWarning)

    return str(uuid4()).replace("-", "").decode("hex")


class _Prod(object):
    """core production configuration"""
    DEBUG = False
    SECRET_KEY = property(fget=lambda self: get_secret_key())


class Debug(_Prod):
    """core debug configuration"""
    DEBUG = True
    SECRET_KEY = get_secret_key(warning=False)


Prod = _Prod()
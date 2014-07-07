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
User
----

Returns information about the current user such as the user name, admin
access, or other related information.
"""

import os
import ctypes


# Linux/mac
try:
    import pwd
except ImportError:  # pragma: no cover
    pwd = NotImplemented

# Windows
try:  # pragma: no cover
    import win32api
    from win32com.shell import shell
except ImportError:  # pragma: no cover
    win32api = NotImplemented

# Shouldn't generally need this, but just in case someone
# has a broken setup we should try
try:
    import getpass
except ImportError:  # pragma: no cover
    getpass = NotImplemented

try:
    from os import getuid
except ImportError:  # pragma: no cover
    getuid = NotImplemented


from pyfarm.agent.sysinfo.system import operating_system
from pyfarm.core.enums import WINDOWS


def username():
    """
    Returns the current user name using the most native api
    we can import. On Linux for example this will use the :mod:`pwd`
    module but on Windows we try to use :mod:`win32api`.
    """
    if pwd is not NotImplemented:
        return pwd.getpwuid(os.getuid())[0]
    elif win32api is not NotImplemented:  # pragma: no cover
        return win32api.GetUserName()
    elif getpass is not NotImplemented:  # pragma: no cover
        return getpass.getuser()
    else:
        raise NotImplementedError("neither `getpass` or `pwd` were imported")


def is_administrator():
    """
    Return True if the current user is root (Linux) or running as an
    Administrator (Windows).
    """
    if getuid is not NotImplemented:
        return getuid() == 0
    elif win32api is not NotImplemented:  # pragma: no cover
        return shell.IsUserAnAdmin()
    elif win32api is NotImplemented and WINDOWS:  # pragma: no cover
        return ctypes.windll.shell32.IsUserAnAdmin() != 0
    else:
        raise NotImplementedError(
            "`is_administrator` is not implemented for %s" % operating_system())

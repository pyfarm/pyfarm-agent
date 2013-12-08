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
SQLite
------

Internal library that the agent can use to interact
with sqlite through :mod:`twisted.enterprise.adbapi`.
"""

from twisted.enterprise.adbapi import ConnectionPool


class SQLiteConnectionPool(ConnectionPool):
    """
    Small wrapper class around :class:`.ConnectionPool` which also provides
    an :meth:`executemany` method.
    """
    reconnect = True
    execute = ConnectionPool.runQuery  # alias

    def __init__(self, path):
        ConnectionPool.__init__(self, "sqlite3", path, check_same_thread=False)

    def _run_executemany(self, trans, *args, **kw):
        trans.executemany(*args, **kw)
        return trans.fetchall()

    def executemany(self, *args, **kwargs):
        """Twisted equivalent of :meth:`sqlite3.Connection.executemany`"""
        return self.runInteraction(self._run_executemany, *args, **kwargs)

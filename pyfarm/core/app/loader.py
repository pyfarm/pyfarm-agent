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
Proxy
=====

Serves as a proxy object
"""

from __future__ import with_statement
import os
import threading
from pyfarm.core.logger import getLogger

logger = getLogger("core.app")


# we want a developer to treat this like an instanced
# object so we're naming it like a normal variable...
class package(object):
    """
    Serves as a proxy object so we are always guaranteed to have
    only on instance of each of the following:
        * :class:`flask.Flask`
        * :class:`flask_sqlalchemy.SQLAlchemy`
        * :class:`flask_security.Security`
        * :class:`flask_admin.Admin`

    Configuration for each class is not defined here however.  For
    configuration information see :cvar:`.CONFIGURATION_MODULES`.

    :cvar CONFIGURATION_MODULES:
        String list of module where we should load configurations
        from
    """
    # configuration data
    THREAD_LOCK = threading.RLock()
    CONFIG_CLASS = os.environ.get("PYFARM_CONFIG", "Debug")
    LOADED_CONFIGURATIONS = []
    CONFIGURATION_MODULES = []

    # internal instances
    _application = None
    _database = None
    _admin = None
    _security = None
    _security_datastore = None

    def __init__(self):
        raise NotImplementedError("this class should not be instanced")

    @classmethod
    def add_config(cls, config, index=None):
        """
        Add configuration to load into :cvar:`.CONFIGURATION_MODULES`.  If
        provided an index `config`` will be added at the provided location
        instead of being appended.
        """
        with cls.THREAD_LOCK:
            logger.debug("adding configuration: %s" % config)
            if index is None:
                cls.CONFIGURATION_MODULES.append(config)
            else:
                cls.CONFIGURATION_MODULES.insert(index, config)

    @classmethod
    def application(cls):
        """Instance or return a :class:`.Flask` application object"""
        with cls.THREAD_LOCK:
            if cls._application is None:
                # import here so we don't break other packages
                # that don't need this
                from flask import Flask
                logger.debug("instancing flask application")
                cls._application = Flask("PyFarm")

            # if any configurations exist, load them
            newly_loaded = []
            configuration_count = len(cls.CONFIGURATION_MODULES)
            for config_template in cls.CONFIGURATION_MODULES[:]:
                config_string = config_template % {"class": cls.CONFIG_CLASS}

                # attempt to load the config
                try:
                    cls._application.config.from_object(config_string)

                except ImportError:
                    logger.debug("cannot import config: %s" % config_string)

                else:
                    cls.CONFIGURATION_MODULES.remove(config_template)
                    cls.LOADED_CONFIGURATIONS.append(config_string)
                    newly_loaded.append(config_string)

            # configurations exist, none were loaded
            if (configuration_count > 0
                and configuration_count == len(cls.CONFIGURATION_MODULES)):
                logger.error("failed to load any configurations")
            elif newly_loaded:
                logger.debug(
                    "loaded configuration(s): %s" % ", ".join(newly_loaded))

        return cls._application

    @classmethod
    def database(cls):
        """Instance or return a :class:`flask_sqlalchemy.SQLAlchemy` object"""
        with cls.THREAD_LOCK:
            if cls._database is None:
                app = cls.application()

                # import here so we don't break other packages
                # that don't need this
                from flask.ext.sqlalchemy import SQLAlchemy
                logger.debug("instancing database")
                cls._database = SQLAlchemy(app)

        return cls._database

    @classmethod
    def security_datastore(cls, User=None, Role=None):
        """
        Instance and return a
        :class:`flask_security.SQLAlchemyUserDatastore` object
        """
        with cls.THREAD_LOCK:
            if cls._security_datastore is None:
                db = cls.database()
                from flask.ext.security import SQLAlchemyUserDatastore
                cls._security_datastore = SQLAlchemyUserDatastore(db,
                                                                  User, Role)

        return cls._security_datastore

    @classmethod
    def security(cls, User=None, Role=None):
        """
        Instance and return a :class:`flask_security.Security` object.
        """
        with cls.THREAD_LOCK:
            if cls._security is None:
                app = cls.application()
                datastore = cls.security_datastore(User=User, Role=Role)
                from flask.ext.security import Security
                cls._security = Security(app, datastore)

        return cls._security

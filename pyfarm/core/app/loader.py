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

import os
from pyfarm.core.logger import getLogger

logger = getLogger("core.app")


# we want a developer to treat this like an instanced
# object so we're naming it like a normal variable...
class package(object):
    """
    Serves as a proxy object so we are always guaranteed to have
    one and only one :class:`flask.Flask` instance.

    :var CONFIGURATION_MODULES:
        String list of module where we should load configurations
        from.  Once a configuration has been loaded the load string
        will be removed from this list and added
        to :var:`LOADED_CONFIGURATIONS`

    .. warning::
        this class is not meant to be instanced
    """
    # configuration data
    CONFIG_CLASS = os.environ.get("PYFARM_CONFIG", "Debug")
    LOADED_CONFIGURATIONS = []
    CONFIGURATION_MODULES = []

    # internal instances
    _application = None
    _database = None
    _admin = None
    _security = None

    def __init__(self):
        raise NotImplementedError("this class should not be instanced")

    @classmethod
    def application(cls):
        """Instance or return a :class:`.Flask` application object"""
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
        else:
            logger.debug(
                "loaded configuration(s): %s" % ".".join(newly_loaded))

        return cls._application

    @classmethod
    def database(cls):
        """Instance or return a :class:`.SQLAlchemy` object"""
        if cls._database is None:
            app = cls.application()

            # import here so we don't break other packages
            # that don't need this
            from flask.ext.sqlalchemy import SQLAlchemy
            logger.debug("instancing database")
            cls._database = SQLAlchemy(app)

        return cls._database
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

import os
import sys
from utcore import unittest
from pyfarm.core.app.loader import package

Flask = None
skip_message = ""
if sys.version_info[0:2] < (2, 6):
    skip_message = "test intended for Python 2.7"
else:
    try:
        from flask import Flask
        from flask.ext.sqlalchemy import SQLAlchemy
        from flask.ext.security import Security, SQLAlchemyUserDatastore
        from flask.ext.admin import Admin
    except ImportError, e:
        Flask = None
        skip_message = "import failure: %s" % e


@unittest.skipIf(Flask is None, skip_message)
class TestPackageLoader(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._application = package._application
        cls._database = package._database
        cls._admin = package._admin
        cls._security = package._security

    def setUp(self):
        del package.CONFIGURATION_MODULES[:]
        del package.LOADED_CONFIGURATIONS[:]
        package._application = self._application
        package._database = self._database
        package._admin = self._security
        package._security = self._security

    def test_config_class_var(self):
        self.assertEqual(
            package.CONFIG_CLASS,
            os.environ.get("PYFARM_CONFIG", "Debug"))

    def test_cannot_instance_class(self):
        with self.assertRaises(NotImplementedError):
            package()

    def test_instance_once(self):
        self.assertIs(package.application(), package.application())
        self.assertIs(package.database(), package.database())

    def test_instance_application(self):
        app = package.application()
        self.assertIs(package._application, app)
        self.assertIsInstance(app, Flask)

    def test_instance_database(self):
        db = package.database()
        self.assertIs(package._database, db)
        self.assertIsInstance(db, SQLAlchemy)

    def test_add_config_append(self):
        package.add_config("foo1")
        package.add_config("foo2")
        self.assertListEqual(package.CONFIGURATION_MODULES, ["foo1", "foo2"])

    def test_add_config_insert(self):
        package.add_config("foo1", 0)
        package.add_config("foo2", 0)
        self.assertListEqual(package.CONFIGURATION_MODULES, ["foo2", "foo1"])

    def test_load_config(self):
        package.add_config("pyfarm.core.app.config.Debug")
        package.application()
        self.assertIn(
            "pyfarm.core.app.config.Debug", package.LOADED_CONFIGURATIONS)
        self.assertNotIn(
            "pyfarm.core.app.config.Debug", package.CONFIGURATION_MODULES)

    def test_load_config_error(self):
        package.add_config("foo")
        package.application()
        self.assertNotIn("foo", package.LOADED_CONFIGURATIONS)
        self.assertIn("foo", package.CONFIGURATION_MODULES)

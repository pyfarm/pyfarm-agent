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
import shutil
import tempfile
from StringIO import StringIO

from voluptuous import Schema, Invalid

from utcore import BaseTestCase
from pyfarm.core.enums import JobTypeLoadMode
from pyfarm.agent.http.assign import PostProcessedSchema, Assign


class TestSchema(BaseTestCase):
    VALID_ASSIGNMENT_DATA = {
        "project": 0, "job": 1, "task": 0,
        "jobtype": {
            "load_type": JobTypeLoadMode.IMPORT,
            "load_from": "",
            "cmd": "", "args": ""},
        "frame": {"start": 1}}

    def setUp(self):
        self.import_dir = tempfile.mkdtemp(prefix="pyfarm-agent-tests-")

        while not os.path.isdir(self.import_dir):
            try:
                os.makedirs(self.import_dir)
            except:
                pass

        with open(os.path.join(self.import_dir, "__init__.py"), "w") as stream:
            stream.write("")

        self.test_data = self.VALID_ASSIGNMENT_DATA.copy()
        sys.path.insert(0, self.import_dir)

    def tearDown(self):
        while self.import_dir in sys.path:
            sys.path.remove(self.import_dir)

        while os.path.isdir(self.import_dir):
            shutil.rmtree(self.import_dir)

    def generate_module(
            self, classname="JobType", subclass_base=True, syntax_error=False,
            call_in_module=False, exec_in_module=False, eval_in_module=False):
        sourcecode = StringIO()

        if subclass_base:
            print >> sourcecode, "from pyfarm.jobtypes.core.jobtype import " \
                                 "JobType as _JobType"
            print >> sourcecode, ""
            base_class = "_JobType"
        else:
            base_class = "object"

        print >> sourcecode, "class %s(%s):" % (classname, base_class)
        print >> sourcecode, "    pass"
        print >> sourcecode, ""

        if syntax_error:
            print >> sourcecode, "a ="

        if call_in_module:
            print >> sourcecode, "int('1')"

        if exec_in_module:
            print >> sourcecode, "exec ''"

        if eval_in_module:
            print >> sourcecode, "eval('{}')"

        module_name = "test_module_%s" % os.urandom(3).encode("hex")
        sourcefile = os.path.join(self.import_dir, module_name) + ".py"

        with open(sourcefile, "w") as sourcefile_stream:
            sourcefile_stream.write(sourcecode.getvalue())

        return ":".join([module_name, classname])

    def test_schema_subclass(self):
        self.assertTrue(issubclass(PostProcessedSchema, Schema))

    def test_instance(self):
        self.assertIsInstance(Assign.SCHEMA, PostProcessedSchema)

    def test_strings_values_only(self):
        self.assertRaises(
            Invalid,
            lambda: PostProcessedSchema.string_keys_and_values({"": None}))
        self.assertRaises(
            Invalid,
            lambda: PostProcessedSchema.string_keys_and_values({None: ""}))

        self.assertRaises(
            Invalid,
            lambda: PostProcessedSchema.string_keys_and_values(None))

    def test_invalid_load_from(self):
        self.test_data["jobtype"]["load_from"] = ""

        with self.assertRaises(Invalid):
            Assign.SCHEMA(self.test_data, {})

    def test_basic_import(self):
        self.test_data["jobtype"]["load_type"] = JobTypeLoadMode.IMPORT
        self.test_data["jobtype"]["load_from"] = self.generate_module()
        assign = Assign.SCHEMA(self.test_data, {})
        self.assertIn("jobtype", assign["jobtype"])


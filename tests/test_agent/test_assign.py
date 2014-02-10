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
import uuid
from functools import partial
try:
    from httplib import OK, UNSUPPORTED_MEDIA_TYPE, BAD_REQUEST, ACCEPTED
except ImportError:
    from http.client import OK, UNSUPPORTED_MEDIA_TYPE, BAD_REQUEST, ACCEPTED

from json import dumps, loads
from StringIO import StringIO

from voluptuous import Schema, Invalid, MultipleInvalid
from pyfarm.agent.testutil import TestCase, dummy_request
from pyfarm.agent.http.assign import PostProcessedSchema, Assign


dummy_post_request = partial(dummy_request, http_method="POST")


class AssignTestBase(TestCase):
    def get_test_data(self):
        return {
            "project": 0, "job": 0, "task": 0,
            "jobtype": {
                "load_from": "import_name:ClassName",
                "cmd": "", "args": ""},
            "frame": {"start": 1}}


class JobTypeValidationBase(AssignTestBase):
    def setUp(self):
        self.import_dir = tempfile.mkdtemp(prefix="pyfarm-agent-tests-")

        while not os.path.isdir(self.import_dir):
            try:
                os.makedirs(self.import_dir)
            except:
                pass

        with open(os.path.join(self.import_dir, "__init__.py"), "w") as stream:
            stream.write("")

        sys.path.insert(0, self.import_dir)

    def tearDown(self):
        while self.import_dir in sys.path:
            sys.path.remove(self.import_dir)

        while os.path.isdir(self.import_dir):
            shutil.rmtree(self.import_dir)

    def generate_module(
            self, classname="JobType", subclass_base=True, syntax_error=False,
            call_in_module=False, exec_in_module=False, eval_in_module=False,
            missing_class=False):
        sourcecode = StringIO()

        if subclass_base:
            print >> sourcecode, "from pyfarm.jobtypes.core.jobtype import " \
                                 "JobType as _JobType"
            print >> sourcecode, ""
            base_class = "_JobType"
        else:
            base_class = "object"

        if not missing_class:
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


class TestSchema(AssignTestBase):
    def test_schema_subclass(self):
        self.assertTrue(issubclass(PostProcessedSchema, Schema))

    def test_instance(self):
        self.assertIsInstance(Assign.SCHEMA, PostProcessedSchema)

    def test_frame_data_population(self):
        test_data = self.get_test_data()
        test_data["frame"] = {"start": 1}
        data = Assign.SCHEMA(test_data, {}, parse_jobtype=False)
        self.assertEqual(data["frame"]["start"], 1)
        self.assertEqual(data["frame"]["end"], 1)
        self.assertEqual(data["frame"]["by"], 1)

    def test_valid_jobtype_load_mode(self):
        test_data = self.get_test_data()
        test_data["jobtype"]["load_type"] = ""
        self.assertRaises(
            MultipleInvalid,
            lambda: Assign.SCHEMA(test_data, {}, parse_jobtype=False))

    def test_strings_values_only(self):
        self.assertEqual(
            PostProcessedSchema.string_keys_and_values({"": ""}),
            {"": ""})
        self.assertRaisesRegexp(
            Invalid, "expected string for env value",
            lambda: PostProcessedSchema.string_keys_and_values({"": None}))
        self.assertRaisesRegexp(
            Invalid, "expected string for env key",
            lambda: PostProcessedSchema.string_keys_and_values({None: ""}))
        self.assertRaisesRegexp(
            Invalid, "invalid type",
            lambda: PostProcessedSchema.string_keys_and_values(None))


class TestJobTypeValidation(JobTypeValidationBase):
    def test_invalid_load_from_format(self):
        test_data = self.get_test_data()
        test_data["jobtype"]["load_from"] = ""
        self.assertRaisesRegexp(
            Invalid, "does not match the 'import_name:ClassName' format",
            lambda: Assign.SCHEMA(test_data, {}))

    def test_invalid_import(self):
        test_data = self.get_test_data()
        test_data["jobtype"]["load_from"] = "a:b"
        self.assertRaisesRegexp(
            Invalid, "no such jobtype module a:b",
            lambda: Assign.SCHEMA(test_data, {}))

    def test_basic_import(self):
        test_data = self.get_test_data()
        test_data["jobtype"]["load_from"] = self.generate_module()
        assign = Assign.SCHEMA(test_data, {})
        self.assertIn("jobtype", assign["jobtype"])

    def test_syntax_error(self):
        test_data = self.get_test_data()
        test_data["jobtype"]["load_from"] = self.generate_module(
            syntax_error=True)
        self.assertRaisesRegexp(
            Invalid, "invalid syntax",
            lambda: Assign.SCHEMA(test_data, {}))

    def test_call_in_module(self):
        test_data = self.get_test_data()
        test_data["jobtype"]["load_from"] = self.generate_module(
            call_in_module=True)
        self.assertRaisesRegexp(
            Invalid, "function calls are not allowed",
            lambda: Assign.SCHEMA(test_data, {}))

    def test_exec_in_module(self):
        test_data = self.get_test_data()
        test_data["jobtype"]["load_from"] = self.generate_module(
            exec_in_module=True)
        self.assertRaisesRegexp(
            Invalid, "function calls are not allowed",
            lambda: Assign.SCHEMA(test_data, {}))

    def test_eval_in_module(self):
        test_data = self.get_test_data()
        test_data["jobtype"]["load_from"] = self.generate_module(
            eval_in_module=True)
        self.assertRaisesRegexp(
            Invalid, "function calls are not allowed",
            lambda: Assign.SCHEMA(test_data, {}))

    def test_missing_class(self):
        test_data = self.get_test_data()
        test_data["jobtype"]["load_from"] = self.generate_module(
            missing_class=True)

        self.assertRaisesRegexp(
            Invalid, "does not have the class",
            lambda: Assign.SCHEMA(test_data, {}))

    def test_invalid_subclass(self):
        test_data = self.get_test_data()
        test_data["jobtype"]["load_from"] = self.generate_module(
            subclass_base=False)

        self.assertRaisesRegexp(
            Invalid, "does not subclass base class",
            lambda: Assign.SCHEMA(test_data, {}))


class TestResource(JobTypeValidationBase):
    templates = os.path.abspath(
        os.path.join(
            os.path.dirname(__file__), "..", "pyfarm", "agent", "templates"))

    def test_get(self):
        view = Assign({"html-templates": self.templates})
        request = dummy_request()
        d = self._render(view, request)

        template_path = os.path.join(self.templates, "pyfarm", "assign.html")
        with open(template_path, "r") as stream:
            template_data = stream.read()

        def rendered(_):
            self.assertEquals(request.responseCode, OK)
            self.assertEquals("".join(request.written), template_data)

        d.addCallback(rendered)
        return d

    def test_post_invalid_content_type(self):
        view = Assign({"html-templates": self.templates})
        request = dummy_post_request()
        d = self._render(view, request)

        def rendered(_):
            self.assertEquals(request.responseCode, UNSUPPORTED_MEDIA_TYPE)
            self.assertIn(
                "only application/json is supported", "".join(request.written))

        d.addCallback(rendered)
        return d

    def test_post_no_data(self):
        view = Assign({"html-templates": self.templates})
        request = dummy_post_request(
            headers={"Content-Type": ["application/json"]})

        d = self._render(view, request)

        def rendered(_):
            self.assertEqual(request.responseCode, BAD_REQUEST)
            self.assertEquals(
                "".join(request.written), dumps("no data provided"))

        d.addCallback(rendered)
        return d

    def test_post_invalid_data(self):
        view = Assign({"html-templates": self.templates})
        request = dummy_post_request(
            headers={"Content-Type": ["application/json"]},
            data="[", json_dumps=False)

        d = self._render(view, request)

        def rendered(_):
            self.assertEqual(request.responseCode, BAD_REQUEST)
            self.assertEquals(
                "".join(request.written),
                dumps("failed to decode [ via json.dumps"))

        d.addCallback(rendered)
        return d

    def test_post_expected_dictionary(self):
        view = Assign({"html-templates": self.templates})
        request = dummy_post_request(
            headers={"Content-Type": ["application/json"]},
            data="")

        d = self._render(view, request)

        def rendered(_):
            self.assertEqual(request.responseCode, BAD_REQUEST)
            self.assertEquals(
                "".join(request.written),
                dumps("expected a dictionary"))

        d.addCallback(rendered)
        return d

    def test_post_success(self):
        test_data = self.get_test_data()
        test_data["jobtype"]["load_from"] = self.generate_module()

        view = Assign({"html-templates": self.templates})
        request = dummy_post_request(
            headers={"Content-Type": ["application/json"]},
            data=test_data)

        d = self._render(view, request)

        def rendered(_):
            self.assertEqual(request.responseCode, ACCEPTED)
            request_result = loads("".join(request.written))

            # success will return a uuid that we can use for tracking
            # which means we should be able to convert it here
            uuid.UUID(request_result)

        d.addCallback(rendered)
        return d

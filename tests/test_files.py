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

from __future__ import with_statement

import os
import uuid
import tempfile

from utcore import TestCase

from pyfarm.core import files


class Expand(TestCase):
    def test_expandpath(self):
        os.environ["FOO"] = "foo"
        joined_files = os.path.join("~", "$FOO")
        expected = os.path.expanduser(os.path.expandvars(joined_files))
        self.assertEqual(files.expandpath(joined_files), expected)

    def test_raise_enverror(self):
        with self.assertRaises(EnvironmentError):
            files.expandenv(str(uuid.uuid4()))

    def test_raise_valuerror(self):
        with self.assertRaises(ValueError):
            var = str(uuid.uuid4())
            os.environ[var] = ""
            files.expandenv(var)

    def test_files_validation(self):
        envvars = {
            "FOO1": self.mktempdir(),
            "FOO2": self.mktempdir(),
            "FOO3": "<unknown_foo>",
            "FOOBARA": os.pathsep.join(["$FOO1", "$FOO2", "$FOO3"])
        }
        os.environ.update(envvars)
        self.assertEqual(
            files.expandenv("FOOBARA"),
            [os.environ["FOO1"], os.environ["FOO2"]]
        )

    def test_files_novalidation(self):
        envvars = {
            "FOO4": self.mktempdir(), 
            "FOO5": self.mktempdir(),
            "FOO6": "<unknown_foo>",
            "FOOBARB": os.pathsep.join(["$FOO5", "$FOO4", "$FOO6"])
        }
        os.environ.update(envvars)
        expanded = files.expandenv("FOOBARB", validate=False)
        self.assertEqual(
            expanded,
            [os.environ["FOO5"], os.environ["FOO4"], os.environ["FOO6"]]
        )


class Which(TestCase):
    def test_which_oserror(self):
        with self.assertRaises(OSError):
            files.which("<FOO>")

    def test_path(self):
        fh, filename = tempfile.mkstemp(
            prefix="pyfarm-", suffix=".sh")

        with open(filename, "w") as stream:
            pass

        os.environ["PATH"] = os.pathsep.join(
            [os.environ["PATH"], os.path.dirname(filename)])
        basename = os.path.basename(filename)
        self.assertEqual(files.which(basename), filename)

    def test_fullfiles(self):
        thisfile = os.path.abspath(__file__)
        self.assertEqual(files.which(thisfile), thisfile)
# No shebang line, this module is meant to be imported
#
# Copyright 2014 Oliver Palmer
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Development
-----------

Contains entry points which constructs the command line tools
used for development such as ``pyfarm-dev-fakerender`` and
``pyfarm-dev-fakework``.
"""

import argparse
import ctypes
import hashlib
import os
import sys
import time
from collections import namedtuple
from functools import partial
from random import choice, randint, random
from textwrap import dedent

import psutil
import requests

from pyfarm.core.enums import NUMERIC_TYPES
from pyfarm.core.utility import convert
from pyfarm.agent.config import config
from pyfarm.agent.logger import getLogger
from pyfarm.agent.utility import dumps


logger = getLogger("agent.cmd")

config["start"] = time.time()


def fake_render():
    process = psutil.Process()
    memory_usage = lambda: convert.bytetomb(process.get_memory_info().rss)
    memory_used_at_start = memory_usage()

    logger.info("sys.argv: %r", sys.argv)

    # build parser
    parser = argparse.ArgumentParser(
        description="Very basic command line tool which vaguely simulates a "
                    "render.")
    parser.add_argument(
        "--ram", type=int, default=25,
        help="How much ram in megabytes the fake command should consume")
    parser.add_argument(
        "--duration", type=int, default=5,
        help="How many seconds it should take to run this command")
    parser.add_argument(
        "--return-code", type=int, action="append", default=[0],
        help="The return code to return, declaring this flag multiple times "
             "will result in a random return code.  [default: %(default)s]")
    parser.add_argument(
        "--duration-jitter", type=int, default=5,
        help="Randomly add or subtract this amount to the total duration")
    parser.add_argument(
        "--ram-jitter", type=int, default=None,
        help="Randomly add or subtract this amount to the ram")
    parser.add_argument(
        "-s", "--start", type=int, required=True,
        help="The start frame.  If no other flags are provided this will also "
             "be the end frame.")
    parser.add_argument(
        "-e", "--end", type=int, help="The end frame")
    parser.add_argument(
        "-b", "--by", type=int, help="The by frame", default=1)
    parser.add_argument(
        "--spew", default=False, action="store_true",
        help="Spews lots of random output to stdout which is generally "
             "a decent stress test for log processing issues.  Do note however "
             "that this will disable the code which is consuming extra CPU "
             "cycles.  Also, use this option with care as it can generate "
             "several gigabytes of data per frame.")
    parser.add_argument(
        "--segfault", action="store_true",
        help="If provided then there's a 25%% chance of causing a segmentation "
             "fault.")
    args = parser.parse_args()

    if args.end is None:
        args.end = args.start

    if args.ram_jitter is None:
        args.ram_jitter = int(args.ram / 2)

    assert args.end >= args.start and args.by >= 1

    random_output = None
    if args.spew:
        random_output = list(os.urandom(1024).encode("hex") for _ in xrange(15))

    errors = 0
    if isinstance(args.start, float):
        logger.warning(
            "Truncating `start` to an integer (float not yet supported)")
        args.start = int(args.start)

    if isinstance(args.end, float):
        logger.warning(
            "Truncating `end` to an integer (float not yet supported)")
        args.end = int(args.end)

    if isinstance(args.by, float):
        logger.warning(
            "Truncating `by` to an integer (float not yet supported)")
        args.by = int(args.by)

    for frame in xrange(args.start, args.end + 1, args.by):
        duration = args.duration + randint(
            -args.duration_jitter, args.duration_jitter)
        ram_usage = max(
            0, args.ram + randint(-args.ram_jitter, args.ram_jitter))
        logger.info("Starting frame %04d", frame)

        # Warn if we're already using more memory
        if ram_usage < memory_used_at_start:
            logger.warning(
                "Memory usage starting up is higher than the value provided, "
                "defaulting --ram to %s", memory_used_at_start)

        # Consume the requested memory (or close to)
        # TODO: this is unrealistic, majority of renders don't eat ram like this
        memory_to_consume = int(ram_usage - memory_usage())
        big_string = " " * 1048576  # ~ 1MB of memory usage
        if memory_to_consume > 0:
            start = time.time()
            logger.debug(
                "Consuming %s megabytes of memory", memory_to_consume)
            try:
                big_string += big_string * memory_to_consume

            except MemoryError:
                logger.error("Cannot render, not enough memory")
                errors += 1
                continue

            logger.debug(
                "Finished consumption of memory in %s seconds.  Off from "
                "target memory usage by %sMB.",
                time.time() - start, memory_usage() - ram_usage)

        # Decently guaranteed to cause a segmentation fault.  Originally from:
        #   https://wiki.python.org/moin/CrashingPython#ctypes
        if args.segfault and random() > .25:
            i = ctypes.c_char('a')
            j = ctypes.pointer(i)
            c = 0
            while True:
                j[c] = 'a'
                c += 1
            j

        # Continually hash a part of big_string to consume
        # cpu cycles
        end_time = time.time() + duration
        last_percent = None
        while time.time() < end_time:
            if args.spew:
                print >> sys.stdout, choice(random_output)
            else:
                hashlib.sha1(big_string[:4096])  # consume CPU cycles

            progress = (1 - (end_time - time.time()) / duration) * 100
            percent, _ = divmod(progress, 5)
            if percent != last_percent:
                last_percent = percent
                logger.info("Progress %03d%%", progress)

        if last_percent is None:
            logger.info("Progress 100%%")

        logger.info("Finished frame %04d in %s seconds", frame, duration)

    if errors:
        logger.error("Render finished with errors")
        sys.exit(1)
    else:
        return_code = choice(args.return_code)
        logger.info("exit %s", return_code)


def fake_work():
    parser = argparse.ArgumentParser(
        description="Quick and dirty script to create a job type, a job, and "
                    "some tasks which are then posted directly to the "
                    "agent.  The primary purpose of this script is to test "
                    "the internal of the job types")
    parser.add_argument(
        "--master-api", default="http://127.0.0.1/api/v1",
        help="The url to the master's api [default: %(default)s]")
    parser.add_argument(
        "--agent-api", default="http://127.0.0.1:50000/api/v1",
        help="The url to the agent's api [default: %(default)s]")
    parser.add_argument(
        "--jobtype", default="FakeRender",
        help="The job type to use [default: %(default)s]")
    parser.add_argument(
        "--job", type=int,
        help="If provided then this will be the job we pull tasks from "
             "and assign to the agent.  Please note we'll only be pulling "
             "tasks that aren't running or assigned.")
    args = parser.parse_args()
    logger.info("Master args.master_api: %s", args.master_api)
    logger.info("Agent args.master_api: %s", args.agent_api)
    assert not args.agent_api.endswith("/")
    assert not args.master_api.endswith("/")

    # session to make requests with
    session = requests.Session()
    session.headers.update({"content-type": "application/json"})

    existing_jobtype = session.get(
        args.master_api + "/jobtypes/%s" % args.jobtype)

    # Create a FakeRender job type if one does not exist
    if not existing_jobtype.ok:
        sourcecode = dedent("""
        from pyfarm.jobtypes.examples import %s as _%s
        class %s(_%s):
            pass""" % (args.jobtype, args.jobtype, args.jobtype, args.jobtype))
        response = session.post(
            args.master_api + "/jobtypes/",
            data=dumps({
                "name": args.jobtype,
                "classname": args.jobtype,
                "code": sourcecode,
                "max_batch": 1}))
        assert response.ok, response.json()
        jobtype_data = response.json()
        logger.info(
            "Created job type %r, id %r", args.jobtype, jobtype_data["id"])

    else:
        jobtype_data = existing_jobtype.json()
        logger.info(
            "Job type %r already exists, id %r",
            args.jobtype, jobtype_data["id"])

    jobtype_version = jobtype_data["version"]

    if args.job is None:
        job = session.post(
            args.master_api + "/jobs/",
            data=dumps({
                "start": 1,
                "end": 1,
                "title": "Fake Job - %s" % int(time.time()),
                "jobtype": args.jobtype}))
        assert job.ok, job.json()
        job = job.json()
        logger.info("Job %r created", job["id"])
    else:
        job = session.get(args.master_api + "/jobs/%s" % args.job)
        if not job.ok:
            logger.error("No such job with id %r", args.job)
            return
        else:
            job = job.json()
            logger.info("Job %r exists", job["id"])

    tasks = session.get(args.master_api + "/jobs/%s/tasks/" % job["id"])
    assert tasks.ok

    job_tasks = []
    for task in tasks.json():
        if task["state"] not in ("queued", "failed"):
            logger.info(
                "Can't use task %s, it's state is not 'queued' or 'failed'",
                task["id"])
            continue

        if task["agent_id"] is not None:
            logger.info(
                "Can't use task %s, it already has an agent assigned",
                task["id"])

        job_tasks.append({"id": task["id"], "frame": task["frame"]})

    if not job_tasks:
        logger.error("Could not find any tasks to send for job %s", job["id"])
        return

    logger.info(
        "Found %s tasks from job %s to assign to %r",
        len(job_tasks), job["id"], args.agent_api)

    assignment_data = {
        "job": {
            "id": job["id"],
            "by": job["by"],
            "ram": job["ram"],
            "ram_warning": job["ram_warning"],
            "ram_max": job["ram_max"],
            "cpus": job["cpus"],
            "batch": job["batch"],
            "user": job["user"],
            "data": job["data"],
            "environ": job["environ"],
            "title": job["title"]},
        "jobtype": {
            "name": args.jobtype,
            "version": jobtype_version},
        "tasks": job_tasks}

    # Drop any keys which don't have values since this
    # would break the schema validation in the agent.
    for key in list(assignment_data["job"]):
        if assignment_data["job"][key] is None:
            del assignment_data["job"][key]

    response = session.post(
        args.agent_api + "/assign",
        data=dumps(assignment_data))
    assert response.ok, response.json()
    logger.info("Tasks posted to agent")


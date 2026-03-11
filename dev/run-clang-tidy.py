#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import print_function
import argparse
import os
import regex
import subprocess
import sys


class string(str):
    def extract(self, rexp):
        return regex.match(rexp, self).group(1)

    def json(self):
        return json.loads(self, object_hook=attrdict)


CODE_CHECKS = """*
    -abseil-*
    -android-*
    -cert-err58-cpp
    -clang-analyzer-osx-*
    -cppcoreguidelines-avoid-c-arrays
    -cppcoreguidelines-avoid-magic-numbers
    -cppcoreguidelines-pro-bounds-array-to-pointer-decay
    -cppcoreguidelines-pro-bounds-pointer-arithmetic
    -cppcoreguidelines-pro-type-reinterpret-cast
    -cppcoreguidelines-pro-type-vararg
    -fuchsia-*
    -google-*
    -hicpp-avoid-c-arrays
    -hicpp-deprecated-headers
    -hicpp-no-array-decay
    -hicpp-use-equals-default
    -hicpp-vararg
    -llvmlibc-*
    -llvm-header-guard
    -llvm-include-order
    -mpi-*
    -misc-non-private-member-variables-in-classes
    -misc-no-recursion
    -misc-unused-parameters
    -modernize-avoid-c-arrays
    -modernize-deprecated-headers
    -modernize-use-nodiscard
    -modernize-use-trailing-return-type
    -objc-*
    -openmp-*
    -readability-avoid-const-params-in-decls
    -readability-convert-member-functions-to-static
    -readability-magic-numbers
    -zircon-*
"""


def run(command, compressed=False, **kwargs):
    if "input" in kwargs:
        input = kwargs["input"]

        if type(input) == list:
            input = "\n".join(input) + "\n"

        kwargs["input"] = input.encode("utf-8")
    reply = subprocess.run(
        command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, **kwargs
    )

    if compressed:
        stdout = gzip.decompress(reply.stdout)
    else:
        stdout = reply.stdout

    stdout = (
        string(stdout.decode("utf-8", errors="ignore").strip())
        if stdout is not None
        else ""
    )
    stderr = (
        string(reply.stderr.decode("utf-8").strip()) if reply.stderr is not None else ""
    )

    if stderr != "":
        print(stderr, file=sys.stderr)

    return reply.returncode, stdout, stderr


def check_list(check_string):
    return ",".join([c.strip() for c in check_string.strip().splitlines() if c.strip()])


CODE_CHECKS = check_list(CODE_CHECKS)


def check_output_has_warnings(output):
    if not output:
        return False
    return bool(regex.search(r"(?i)\b(warning|error):", output))


def ensure_compile_db(build_dir):
    path = os.path.join(build_dir, "compile_commands.json")
    return os.path.exists(path)


def tidy(args):
    build_dir = "cpp/build/"

    if not ensure_compile_db(build_dir):
        print("No compile_commands.json found in '{}'.".format(build_dir))
        return 1

    fix = "--fix" if args.fix == "fix" else ""
    files = args.files

    cmd = (
        "xargs clang-tidy -p={build} --format-style=file "
        "--checks='{checks}' {fix} --quiet".format(
            build=build_dir,
            checks=CODE_CHECKS,
            fix=fix,
        )
    )

    print("Running clang-tidy on {} file(s)...".format(len(files)))
    status, stdout, stderr = run(cmd, input=files)

    combined_output = ""
    if stdout:
        combined_output += stdout + "\n"
    if stderr:
        combined_output += stderr

    if check_output_has_warnings(combined_output):
        print("clang-tidy found warnings/errors.")
        # Print a reasonably sized portion to avoid flooding logs; print all if small
        if len(combined_output) > 20000:
            print(combined_output[:20000])
            print("... (output truncated)")
        else:
            print(combined_output)
        return 1

    # Also check the return code (status) - clang-tidy may return non-zero for internal errors
    if status != 0:
        print("clang-tidy returned non-zero exit code:", status)
        if combined_output:
            print(combined_output)
        return 1

    print("clang-tidy completed successfully (no warnings).")
    return 0


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run clang-tidy with project settings (compatible with older Python versions)"
    )
    parser.add_argument("--fix", action="store_const", default="show", const="fix")
    parser.add_argument("--commit", default="", help="Commit for check")
    parser.add_argument("files", metavar="FILES", nargs="*", help="files to process")
    return parser.parse_args()


def main():
    args = parse_args()
    if not args.files:
        args.files = [line.strip() for line in sys.stdin if line.strip()]
    return tidy(args)


if __name__ == "__main__":
    sys.exit(main())

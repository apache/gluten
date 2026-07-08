#!/usr/bin/env python3
# Copyright(c) 2021-2023 Intel Corporation.
# Copyright (c) Facebook, Inc. and its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import argparse
import os
import sys

import util

SCRIPTS = util.script_path()


def header_command(commit, files, fix):
    options = "-vk" if fix == "show" else "-iv"

    status, stdout, stderr = util.run(
        f"{SCRIPTS}/license-header.py {options} -", input=files
    )

    if stdout != "":
        print(stdout)

    return status


def get_commit(files):
    if files == "commit":
        return "HEAD^"

    if files == "main" or files == "master":
        return util.run(f"git merge-base origin/{files} HEAD")[1]

    if files == "branch":
        return os.environ["BASE_COMMIT"]

    return ""


def get_files(commit, path):
    filelist = []

    if commit != "":
        status, stdout, stderr = util.run(
            f"git diff --relative --name-only --diff-filter='ACM' {commit}"
        )
        filelist = stdout.splitlines()
    else:
        for root, dirs, files in os.walk(path):
            for name in files:
                filelist.append(os.path.join(root, name))

    return [file for file in filelist if "ep/build-velox" not in file]


def help(args):
    parser.print_help()
    return 0


def add_check_options(subparser, name):
    parser = subparser.add_parser(name)
    parser.add_argument("--fix", action="store_const", default="show", const="fix")
    return parser


def add_options(parser):
    files = parser.add_subparsers(dest="files")

    tree_parser = add_check_options(files, "tree")
    tree_parser.add_argument("path", default="")

    branch_parser = add_check_options(files, "main")
    branch_parser = add_check_options(files, "master")
    branch_parser = add_check_options(files, "branch")
    commit_parser = add_check_options(files, "commit")


def add_check_command(parser, name):
    subparser = parser.add_parser(name)
    add_options(subparser)

    return subparser


def parse_args():
    global parser
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawTextHelpFormatter,
        description="""Check header

    check.py header {commit,main,master} [--fix]
    check.py header {tree} [--fix] PATH
""",
    )
    command = parser.add_subparsers(dest="command")
    command.add_parser("help")

    header_command_parser = add_check_command(command, "header")

    parser.set_defaults(path="")
    parser.set_defaults(command="help")

    return parser.parse_args()


def run_command(args, command):
    commit = get_commit(args.files)
    files = get_files(commit, args.path)
    return command(commit, files, args.fix)


def header(args):
    return run_command(args, header_command)


def main():
    args = parse_args()
    return globals()[args.command](args)


if __name__ == "__main__":
    sys.exit(main())

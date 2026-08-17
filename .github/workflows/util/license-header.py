#!/usr/bin/env python3
# Copyright(c) 2021-2023 Intel Corporation.
# Copyright (c) Facebook, Inc. and its affiliates.
#
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

import argparse
from collections import OrderedDict
import fnmatch
import os
import regex
import sys

import util

SCRIPTS = util.script_path()


class attrdict(dict):
    __getattr__ = dict.__getitem__
    __setattr__ = dict.__setitem__


def parse_args():
    parser = argparse.ArgumentParser(description="Update license headers")
    parser.add_argument(
        "--header", default=f"{SCRIPTS}/license.header", help="license header file"
    )
    parser.add_argument(
        "--extra",
        default=80,
        help="extra characters past beginning of file to look for header",
    )
    parser.add_argument(
        "--editdist", default=12, type=int, help="max edit distance between headers"
    )
    parser.add_argument(
        "--remove", default=False, action="store_true", help="remove the header"
    )
    parser.add_argument(
        "--cslash",
        default=False,
        action="store_true",
        help='use C slash "//" style comments',
    )
    parser.add_argument(
        "-v", default=False, action="store_true", dest="verbose", help="verbose output"
    )
    parser.add_argument(
        "--excluded_copyright_files",
        default=f"{SCRIPTS}/excluded_copyright_files.txt",
        help="Files that should be excluded",
    )

    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "-k", default=False, action="store_true", dest="check", help="check headers"
    )
    group.add_argument(
        "-i",
        default=False,
        action="store_true",
        dest="inplace",
        help="edit file inplace",
    )

    parser.add_argument("files", metavar="FILES", nargs="+", help="files to process")

    return parser.parse_args()


def file_read(filename):
    with open(filename) as file:
        return file.read()


def file_lines(filename):
    return file_read(filename).rstrip().split("\n")


def wrapper(prefix, leader, suffix, header):
    return prefix + "\n".join([leader + line for line in header]) + suffix


def wrapper_chpp(header, args):
    if args.cslash:
        return wrapper("", "//", "\n", header)
    else:
        return wrapper("/*\n", " *", "\n */\n", header)


def wrapper_hash(header, args):
    return wrapper("", "#", "\n", header)


def wrapper_tilde(header, args):
    return wrapper("<!--\n", "  ~", "\n  -->\n", header)


file_types = OrderedDict(
    {
        "CMakeLists.txt": attrdict({"wrapper": wrapper_hash, "hashbang": False}),
        "Makefile": attrdict({"wrapper": wrapper_hash, "hashbang": False}),
        "*.cpp": attrdict({"wrapper": wrapper_chpp, "hashbang": False}),
        "*.cc": attrdict({"wrapper": wrapper_chpp, "hashbang": False}),
        "*.c": attrdict({"wrapper": wrapper_chpp, "hashbang": False}),
        # Matches 'Dockerfile' and suffixed variants such as 'Dockerfile.centos9-static-build'.
        "Dockerfile*": attrdict({"wrapper": wrapper_hash, "hashbang": False}),
        "*.h": attrdict({"wrapper": wrapper_chpp, "hashbang": False}),
        "*.hpp": attrdict({"wrapper": wrapper_chpp, "hashbang": False}),
        "*.inc": attrdict({"wrapper": wrapper_chpp, "hashbang": False}),
        "*.js": attrdict({"wrapper": wrapper_chpp, "hashbang": False}),
        "*.java": attrdict({"wrapper": wrapper_chpp, "hashbang": False}),
        "*.scala": attrdict({"wrapper": wrapper_chpp, "hashbang": False}),
        "*.prolog": attrdict({"wrapper": wrapper_chpp, "hashbang": False}),
        "*.py": attrdict({"wrapper": wrapper_hash, "hashbang": True}),
        "*.sh": attrdict({"wrapper": wrapper_hash, "hashbang": True}),
        "*.thrift": attrdict({"wrapper": wrapper_chpp, "hashbang": False}),
        "*.yml": attrdict({"wrapper": wrapper_hash, "hashbang": False}),
        "*.yaml": attrdict({"wrapper": wrapper_hash, "hashbang": False}),
        # The XML declaration must stay the first thing in the document, so the header
        # goes after it rather than at the top of the file.
        "*.xml": attrdict(
            {
                "wrapper": wrapper_tilde,
                "hashbang": False,
                "prologue": r"^<\?xml.*?\?>[ \t]*\n",
            }
        ),
    }
)

file_pattern = regex.compile(
    "|".join(["^" + fnmatch.translate(type) + "$" for type in file_types.keys()])
)


def get_filename(filename):
    return os.path.basename(filename)


def get_fileextn(filename):
    split = os.path.splitext(filename)
    if len(split) <= 1:
        return ""

    return split[-1]


def get_wrapper(filename):
    if filename in file_types:
        return file_types[filename]

    extension = "*" + get_fileextn(filename)
    if extension in file_types:
        return file_types[extension]

    # Fall back to glob matching, for keys such as 'Dockerfile*' that are neither an
    # exact name nor an extension.
    for pattern, attributes in file_types.items():
        if fnmatch.fnmatch(filename, pattern):
            return attributes

    return None


def message(file, string):
    if file:
        print(string, file=file)


def check_license_header(files, license_header, args):
    global fail
    global log_to
    for filepath in files:
        filename = get_filename(filepath)

        matched = file_pattern.match(filename)

        if not matched:
            message(log_to, "Skip : " + filepath)
            continue

        content = file_read(filepath)
        wrap = get_wrapper(filename)

        header_comment = wrap.wrapper(license_header, args)

        start = 0
        end = 0

        # Look for an exact substr match
        #
        found = content.find(header_comment, 0, len(header_comment) + args.extra)
        if found >= 0:
            if not args.remove:
                message(log_to, "OK   : " + filepath)
                continue

            start = found
            end = found + len(header_comment)
        else:
            # Look for a fuzzy match in the first 60 chars
            #
            found = regex.search(
                "(?be)(%s){e<=%d}" % (regex.escape(header_comment[0:60]), 6),
                content[0 : 80 + args.extra],
            )
            if found:
                fuzzy = regex.compile(
                    "(?be)(%s){e<=%d}" % (regex.escape(header_comment), args.editdist)
                )

                # If the first 80 chars match - try harder for the rest of the header
                #
                found = fuzzy.search(
                    content[0 : len(header_comment) + args.extra], found.start()
                )
                if found:
                    start = found.start()
                    end = found.end()

        if args.remove:
            if start == 0 and end == 0:
                if not args.inplace:
                    print(content, end="")

                message(log_to, "OK   : " + filepath)
                continue

            # If removing the header text, zero it out there.
            header_comment = ""

        message(log_to, "Fix  : " + filepath)

        if args.check:
            fail = True
            continue

        # Remove any partially matching header
        #
        content = content[0:start] + content[end:]

        # Some formats require a line to stay first: a shebang for scripts, or the
        # declaration for XML. Insert the header after it when present.
        prologue = r"^#!.*\n" if wrap.hashbang else wrap.get("prologue")
        search = regex.search(prologue, content) if prologue else None

        if search:
            content = (
                content[search.start() : search.end()]
                + header_comment
                + content[search.end() :]
            )
        else:
            content = header_comment + content

        if args.inplace:
            with open(filepath, "w") as file:
                print(content, file=file, end="")
        else:
            print(content, end="")


def process_license_header(files, args):
    excluded_copyright_files_globs = []
    if args.excluded_copyright_files:
        with open(args.excluded_copyright_files) as f:
            excluded_copyright_files_globs.extend(line.strip() for line in f)

    need_check_copyright_files = []
    for file in files:
        if any(
            [fnmatch.fnmatch(file, glob) for glob in excluded_copyright_files_globs]
        ):
            continue
        else:
            need_check_copyright_files.append(file)

    license_header = file_lines(args.header)
    check_license_header(need_check_copyright_files, license_header, args)


fail = False
log_to = None


def main():
    global fail
    global log_to

    args = parse_args()

    if args.verbose:
        log_to = sys.stderr

    if args.check:
        log_to = None

        if args.verbose:
            log_to = sys.stdout

    if len(args.files) == 1 and args.files[0] == "-":
        files = [file.strip() for file in sys.stdin.readlines()]
    else:
        files = args.files

    process_license_header(files, args)

    if fail:
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())

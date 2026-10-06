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
#

"""Compare the velox-backend-*-function-support.md docs in the working tree against a git revision (default HEAD).

Usage: python3 tools/scripts/diff-function-support-docs.py [REV]   (run from the Gluten repo root)
"""

import subprocess
import sys

REV = sys.argv[1] if len(sys.argv) > 1 else "HEAD"


def rows(text):
    d = {}
    for line in text.splitlines():
        if (
            not line.startswith("| ")
            or line.startswith("| Spark Functions")
            or line.startswith("|--")
        ):
            continue
        c = [x.strip() for x in line.strip().strip("|").split("|")]
        if len(c) >= 4:
            d[c[0]] = (c[2] or "-", c[3])
    return d


def header(text):
    return next((l for l in text.splitlines() if l.startswith("**Out of")), "")


for cat in ["scalar", "aggregate", "window", "generator"]:
    path = f"docs/velox-backend-{cat}-function-support.md"
    old_text = subprocess.run(
        ["git", "show", f"{REV}:{path}"], capture_output=True, text=True
    ).stdout
    new_text = open(path).read()
    old, new = rows(old_text), rows(new_text)
    print(f"\n== {cat}: {len(old)} -> {len(new)}")
    print(f"  OLD: {header(old_text)}\n  NEW: {header(new_text)}")
    for k in sorted(old):
        if k in new and old[k][0] != new[k][0]:
            print(f"  {k}: {old[k][0]} -> {new[k][0]}")
        elif k in new and old[k][1] != new[k][1]:
            print(f"  {k}: restriction {old[k][1]!r} -> {new[k][1]!r}")
    for st in ["S", "PS", "-"]:
        ks = sorted(k for k in new if k not in old and new[k][0] == st)
        if ks:
            print(f"  NEW [{st}] ({len(ks)}): " + ", ".join(ks))
    removed = sorted(k for k in old if k not in new)
    if removed:
        print("  REMOVED: " + ", ".join(removed))

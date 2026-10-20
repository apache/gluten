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

"""Methods whose results are reported but excluded from the Iceberg gate."""

import csv
from pathlib import Path

FLAKY_TESTS = Path(__file__).with_name("flaky-tests.csv")


def load_flaky_tests(path=FLAKY_TESTS):
    tests = set()
    with path.open(newline="") as source:
        reader = csv.reader(
            (line for line in source if not line.lstrip().startswith("#")), strict=True
        )
        if next(reader, None) != ["class", "method"]:
            raise ValueError(
                f"Expected class,method header in flaky test whitelist: {path}"
            )
        for row in reader:
            if not row:
                continue
            key = tuple(value.strip() for value in row)
            if len(key) != 2 or not all(key):
                raise ValueError(f"Invalid flaky test whitelist entry: {path}: {row}")
            if key in tests:
                raise ValueError(f"Duplicate flaky test whitelist entry: {path}: {row}")
            tests.add(key)
    return tests


def is_flaky(row, tests):
    return (row.get("class"), row.get("name", "").partition("(")[0]) in tests

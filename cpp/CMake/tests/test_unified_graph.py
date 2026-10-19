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

import contextlib
import io
import json
from pathlib import Path
import tempfile
import unittest

from check_unified_graph import check


class UnifiedGraphTest(unittest.TestCase):
    def test_cmake_boolean_values(self):
        enabled = ("ON", "TRUE", "YES", "Y", "1", "2", "on", "true", "yes")
        disabled = ("OFF", "FALSE", "NO", "N", "0", "IGNORE", "NOTFOUND", "",
                    "GTest-NOTFOUND", "off", "false", "no", "gtest-notfound")
        options = {
            "BUILD_TESTS": ("velox_plan_conversion_test", "gluten_velox_memory_test"),
            "BUILD_BENCHMARKS": ("delta_bitmap_benchmark",),
            "VELOX_BUILD_TESTING": ("velox_memory_test",),
            "VELOX_ENABLE_BENCHMARKS": ("velox_common_indexed_priority_queue_benchmark",),
        }
        with tempfile.TemporaryDirectory() as directory:
            build = Path(directory)
            reply = build / ".cmake/api/v1/reply"
            reply.mkdir(parents=True)
            (reply / "index-test.json").write_text(json.dumps({
                "reply": {"codemodel-v2": {"jsonFile": "codemodel.json"}},
            }))
            targets = {
                "velox": {"type": "STATIC_LIBRARY"},
                "gluten": {
                    "type": "SHARED_LIBRARY",
                    "link": {"commandFragments": [{"fragment": "libgflags.a"}]},
                },
                "gluten_velox_backend": {"type": "SHARED_LIBRARY", "sources": []},
                "gluten_core_objects": {"type": "OBJECT_LIBRARY", "paths": {"build": "core"}},
                "gluten_velox_objects": {"type": "OBJECT_LIBRARY"},
            }
            for name, target in targets.items():
                (reply / f"{name}.json").write_text(json.dumps(target))
            for option, optional_targets in options.items():
                for value in enabled + disabled:
                    with self.subTest(option=option, value=value):
                        names = list(targets)
                        if value in enabled:
                            names.extend(optional_targets)
                        (reply / "codemodel.json").write_text(json.dumps({
                            "configurations": [{"targets": [
                                {"name": name, "jsonFile": f"{name}.json"}
                                for name in names
                            ]}],
                        }))
                        (build / "CMakeCache.txt").write_text("".join(
                            f"{name}:BOOL={value if name == option else 'OFF'}\n"
                            for name in options
                        ))
                        with contextlib.redirect_stdout(io.StringIO()):
                            check(build)


if __name__ == "__main__":
    unittest.main()

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

import os
from pathlib import Path
import platform
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[3]
VCPKG_ENV = {
    "VCPKG_ROOT": str(ROOT / "dev/vcpkg/.vcpkg"),
    "VCPKG_TRIPLET": os.environ.get(
        "VCPKG_TRIPLET",
        "arm64-linux-neon" if platform.machine().lower() in ("aarch64", "arm64")
        else "x64-linux-avx",
    ),
    "VCPKG_MANIFEST_DIR": str(ROOT / "dev/vcpkg"),
}


class ToolchainTest(unittest.TestCase):
    def configure(self, directory, options=(), overrides=None):
        source = directory / "cpp"
        source.mkdir()
        (directory / "dev").symlink_to(ROOT / "dev", target_is_directory=True)
        prefix = (ROOT / "cpp/CMakeLists.txt").read_text().split("project(gluten)")[0]
        (source / "CMakeLists.txt").write_text(
            prefix + "project(toolchain_probe LANGUAGES CXX)\n"
            'file(WRITE "${CMAKE_BINARY_DIR}/selected.txt" "${CMAKE_TOOLCHAIN_FILE}\\n")\n'
        )
        env = {key: value for key, value in os.environ.items()
               if not key.startswith(("VCPKG_", "GLUTEN_VCPKG", "CMAKE_"))
               and key not in ("CPU_TARGET", "CC", "CXX")}
        env.update(overrides or {})
        build = directory / "build"
        result = subprocess.run(
            ["cmake", "-S", str(source), "-B", str(build), "-G", "Ninja", *options],
            env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        )
        self.assertEqual(result.returncode, 0, result.stdout)
        return (build / "selected.txt").read_text().strip(), (build / "CMakeCache.txt").read_text()

    def test_vcpkg_dependency_defaults_and_overrides(self):
        toolchain = ROOT / "dev/vcpkg/toolchain.cmake"
        for overrides in ([], ["-DVELOX_DEPENDENCY_SOURCE=AUTO", "-DVELOX_GFLAGS_TYPE=shared"]):
            with self.subTest(overrides=overrides), tempfile.TemporaryDirectory() as directory:
                _, cache = self.configure(Path(directory), [
                    f"-DCMAKE_TOOLCHAIN_FILE={toolchain}", *overrides,
                ], VCPKG_ENV)
                source = "AUTO" if overrides else "SYSTEM"
                linkage = "shared" if overrides else "static"
                self.assertIn(f"VELOX_DEPENDENCY_SOURCE:STRING={source}", cache)
                self.assertIn(f"VELOX_GFLAGS_TYPE:STRING={linkage}", cache)

    def test_explicit_toolchain_with_environment(self):
        with tempfile.TemporaryDirectory() as directory:
            selected, cache = self.configure(Path(directory), [
                f"-DCMAKE_TOOLCHAIN_FILE={ROOT / 'dev/vcpkg/toolchain.cmake'}",
            ], VCPKG_ENV)
            self.assertTrue(selected, "Explicit toolchain was not selected")
            self.assertTrue(Path(selected).samefile(ROOT / "dev/vcpkg/toolchain.cmake"))
            self.assertIn(f"VCPKG_TARGET_TRIPLET:STRING={VCPKG_ENV['VCPKG_TRIPLET']}", cache)
            installed = ROOT / "dev/vcpkg/vcpkg_installed"
            self.assertIn(f"VCPKG_INSTALLED_DIR:PATH={installed}", cache)
            self.assertIn("VCPKG_MANIFEST_MODE:BOOL=OFF", cache)

    def test_custom_toolchain_and_no_toolchain(self):
        for mode in ("none", "argument", "environment", "argument_over_environment"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory:
                directory = Path(directory)
                custom = directory / "custom.cmake"
                custom.write_text("set(CUSTOM_TOOLCHAIN_USED ON CACHE BOOL \"\")\n")
                invalid = directory / "must-not-be-loaded.cmake"
                options, env = [], {}
                if mode == "none":
                    pass
                elif mode == "environment":
                    env["CMAKE_TOOLCHAIN_FILE"] = str(custom)
                else:
                    options.append(f"-DCMAKE_TOOLCHAIN_FILE={custom}")
                    if mode == "argument_over_environment":
                        env["CMAKE_TOOLCHAIN_FILE"] = str(invalid)
                selected, cache = self.configure(directory, options, env)
                if mode == "none":
                    self.assertEqual(selected, "")
                    self.assertNotIn("VCPKG_MANIFEST_MODE:", cache)
                else:
                    self.assertEqual(selected, str(custom))
                    self.assertIn("CUSTOM_TOOLCHAIN_USED:BOOL=ON", cache)

    def test_vcpkg_environment_paths_survive_compiler_checks(self):
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            external = directory / "external"
            external.mkdir()
            for path in ("scripts", ".vcpkg-root"):
                (external / path).symlink_to(ROOT / "dev/vcpkg/.vcpkg" / path)
            # No .vcpkg beside this copied toolchain: try_compile inherits the environment.
            toolchain = directory / "toolchain.cmake"
            toolchain.write_bytes((ROOT / "dev/vcpkg/toolchain.cmake").read_bytes())
            manifest = directory / "manifest"
            manifest.mkdir()
            installed = manifest / "vcpkg_installed"
            installed.symlink_to(ROOT / "dev/vcpkg/vcpkg_installed", target_is_directory=True)
            selected, cache = self.configure(directory, [
                f"-DCMAKE_TOOLCHAIN_FILE={toolchain}",
            ], {
                **VCPKG_ENV,
                "VCPKG_ROOT": str(external),
                "VCPKG_MANIFEST_DIR": str(manifest),
            })
            self.assertEqual(selected, str(toolchain))
            self.assertIn(f"Z_VCPKG_ROOT_DIR:INTERNAL={external}", cache)
            self.assertIn(f"VCPKG_INSTALLED_DIR:PATH={installed}", cache)
            self.assertIn(f"VCPKG_TARGET_TRIPLET:STRING={VCPKG_ENV['VCPKG_TRIPLET']}", cache)


if __name__ == "__main__":
    unittest.main()

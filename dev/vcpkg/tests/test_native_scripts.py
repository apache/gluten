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
import shutil
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[3]


class NativeScriptTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.build = self.root / "cpp/build"
        self.velox = self.root / "ep/build-velox/build/velox_ep"
        self.log = self.root / "commands.log"
        for script in ("builddeps-veloxbe.sh", "builddep-veloxbe-inc.sh",
                       "buildbundle-veloxbe.sh", "package-vcpkg.sh"):
            path = self.root / "dev" / script
            path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(ROOT / "dev" / script, path)
        self.build.mkdir(parents=True)
        (self.build / "keep").write_text("keep\n")
        self.velox.mkdir(parents=True)
        (self.velox / "CMakeLists.txt").write_text("# Prepared Velox\n")
        (self.velox / "keep").write_text("keep\n")
        self.write("dev/build-helper-functions.sh", """
install_librdkafka() { printf 'install_librdkafka\\n' >> "$COMMAND_LOG"; }
setup_linux() { printf 'setup_linux\\n' >> "$COMMAND_LOG"; }
setup_macos() { printf 'setup_macos\\n' >> "$COMMAND_LOG"; }
install_adapters() { printf 'install_adapters\\n' >> "$COMMAND_LOG"; }
""")
        self.write("ep/build-velox/build/velox_ep/scripts/setup-common.sh", "# Stub\n")
        self.write("dev/build-arrow.sh", 'printf "build_arrow\\n" >> "$COMMAND_LOG"\n')
        self.write("ep/build-velox/src/get-velox.sh", """#!/bin/bash
printf 'get_velox|%s\\n' "$*" >> "$COMMAND_LOG"
mkdir -p "$VELOX_HOME/scripts"
printf '# Prepared Velox\\n' > "$VELOX_HOME/CMakeLists.txt"
printf '# Stub\\n' > "$VELOX_HOME/scripts/setup-common.sh"
""")
        self.write("ep/build-velox/src/build-velox.sh", """#!/bin/bash
printf 'standalone|%s\\n' "$*" >> "$COMMAND_LOG"
""")
        self.write("dev/vcpkg/env.sh", """
printf 'env|%s\\n' "$*" >> "$COMMAND_LOG"
if [ "${1:-}" != --skip-install ]; then
  vcpkg "$@" || return $?
fi
export GLUTEN_VCPKG_ENABLED=stub
export VCPKG_ROOT="$SCRIPT_ROOT/dev/vcpkg/.vcpkg"
export VCPKG_TRIPLET=x64-linux-avx
export VCPKG_MANIFEST_DIR="$SCRIPT_ROOT/dev/vcpkg"
export CMAKE_TOOLCHAIN_FILE="$SCRIPT_ROOT/dev/vcpkg/toolchain.cmake"
""")
        for name in ("cmake", "ninja", "vcpkg"):
            self.write(f"bin/{name}", """#!/bin/bash
name=${0##*/}
printf '%s|%s\\n' "$name" "$*" >> "$COMMAND_LOG"
if [ "$name" = ninja ] || { [ "$name" = cmake ] && [ "${1:-}" = --build ]; }; then
  [ "${FAIL_BUILD:-0}" = 0 ] || exit 42
  mkdir -p "$SCRIPT_ROOT/cpp/build/releases"
  printf 'fresh gluten\\n' > "$SCRIPT_ROOT/cpp/build/releases/libgluten.so"
  printf 'fresh backend\\n' > "$SCRIPT_ROOT/cpp/build/releases/libvelox.so"
fi
""")
        self.write("build/mvn", """#!/bin/bash
printf 'mvn|%s\\n' "$*" >> "$COMMAND_LOG"
case "$*" in
  *-Dexpression=platform*) printf 'linux\\n' ;;
  *-Dexpression=arch*) printf 'amd64\\n' ;;
esac
""")
        for name in ("git", "sudo", "curl", "wget", "make", "dnf", "yum"):
            self.write(f"bin/{name}", '#!/bin/bash\nprintf "Unexpected command: %s\\n" "$0" >&2\nexit 99\n')
        self.env = {key: value for key, value in os.environ.items()
                    if not key.startswith(("VCPKG", "GLUTEN_VCPKG", "CMAKE_"))
                    and key not in ("BUILD_TYPE", "SCALA_VERSION", "VELOX_HOME", "INSTALL_PREFIX",
                                    "CPU_TARGET", "BASH_ENV", "ENV", "FAIL_BUILD")}
        self.env.update(PATH=f"{self.root}/bin:{os.environ['PATH']}",
                        COMMAND_LOG=str(self.log), SCRIPT_ROOT=str(self.root))

    def write(self, name, content):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
        path.chmod(0o755)

    def cache(self, **options):
        values = {"CMAKE_BUILD_TYPE": "Debug", "velox_SOURCE_DIR": str(self.velox),
                  "BUILD_TESTS": "OFF", "BUILD_BENCHMARKS": "OFF"}
        values.update(options)
        (self.build / "CMakeCache.txt").write_text("".join(
            f"{name}:STRING={value}\n" for name, value in values.items()))
        (self.build / "build.ninja").write_text("# Stub graph\n")

    def run_script(self, script, *args, success=True, **env):
        self.log.write_text("")
        result = subprocess.run(["bash", str(self.root / "dev" / script), *args],
                                cwd=self.root, env=dict(self.env, **env), text=True,
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        if success:
            self.assertEqual(result.returncode, 0, result.stdout)
        else:
            self.assertNotEqual(result.returncode, 0, result.stdout)
        return self.log.read_text().splitlines()

    def main(self, *args, **kwargs):
        return self.run_script("builddeps-veloxbe.sh", "--run_setup_script=OFF",
                               "--build_arrow=OFF", "--num_threads=3", *args, **kwargs)

    def test_default_build_uses_one_graph_and_keeps_clean_build(self):
        for mode in ("ON", "OFF"):
            with self.subTest(vcpkg=mode):
                (self.build / "keep").write_text("previous build\n")
                commands = self.main(f"--enable_vcpkg={mode}", "--build_tests=ON",
                                     "--build_benchmarks=ON", "--build_examples=ON",
                                     "--build_velox_tests=ON", "--build_velox_benchmarks=ON")
                self.assertFalse(any(c.startswith(("standalone|", "get_velox|")) for c in commands))
                configure = [c for c in commands if c.startswith("cmake|")]
                self.assertEqual(len(configure), 1, commands)
                for option in ("BUILD_TESTS", "BUILD_BENCHMARKS", "BUILD_EXAMPLES",
                               "VELOX_BUILD_TESTING", "VELOX_ENABLE_BENCHMARKS"):
                    self.assertIn(f"-D{option}=ON", configure[0])
                self.assertIn("-DCMAKE_GTEST_DISCOVER_TESTS_DISCOVERY_MODE=PRE_TEST", configure[0])
                self.assertEqual([c for c in commands if c.startswith("ninja|")], ["ninja|-j 3"])
                self.assertFalse((self.build / "keep").exists())
                self.assertEqual((self.velox / "keep").read_text(), "keep\n")

    def test_source_preparation_and_explicit_commands(self):
        shutil.rmtree(self.velox)  # Only the disposable fixture, never the actual checkout.
        commands = self.main()
        self.assertEqual(sum(c.startswith("get_velox|") for c in commands), 1)
        for command, prefix in (("get_velox", "get_velox|"), ("build_velox", "standalone|"),
                                ("build_arrow", "build_arrow"), ("install_librdkafka", "install_librdkafka")):
            with self.subTest(command=command):
                calls = self.main(command)
                self.assertTrue(any(c.startswith(prefix) for c in calls), calls)
                self.assertFalse(any(c.startswith(("cmake|", "ninja|")) for c in calls))

    def test_gpu_options_are_passed_to_clean_build(self):
        self.cache(CMAKE_CUDA_ARCHITECTURES="90", CMAKE_CUDA_COMPILER="/custom/nvcc", CMAKE_CUDA_FLAGS="")
        commands = self.main("--enable_gpu=ON", "build_gluten_cpp")
        configure = next(c for c in commands if c.startswith("cmake|"))
        for option in ("CMAKE_CUDA_ARCHITECTURES=75", "CMAKE_CUDA_COMPILER=/usr/local/cuda/bin/nvcc",
                       "CMAKE_CUDA_FLAGS=-Xcompiler=-fno-access-control"):
            self.assertIn(f"-D{option}", configure)
        self.assertFalse((self.build / "CMakeCache.txt").exists())
        commands = self.main("build_gluten_cpp")
        self.assertNotIn("-DCMAKE_CUDA_", next(c for c in commands if c.startswith("cmake|")))

    def test_vcpkg_keeps_original_build_tests_option(self):
        for value in ("ON", "OFF"):
            with self.subTest(build_tests=value):
                calls = self.main("--enable_vcpkg=ON", f"--build_tests={value}",
                                  "--build_benchmarks=ON", "build_gluten_cpp")
                self.assertIn(f"--build_tests={value}", next(c for c in calls if c.startswith("vcpkg|")))
                self.assertIn(f"-DBUILD_TESTS={value}", next(c for c in calls if c.startswith("cmake|")))

    def test_incremental_only_builds_backend_then_copies(self):
        self.cache()
        calls = self.run_script("builddep-veloxbe-inc.sh", "--num_threads=3", "--scala_version=2.12")
        self.assertEqual(calls[0], "env|--skip-install")
        builds = [c for c in calls if c.startswith("cmake|")]
        self.assertEqual(len(builds), 1, calls)
        self.assertIn("--target gluten_velox_backend -j 3", builds[0])
        libraries = list((self.root / "backends-velox/target/scala-2.12/classes").glob("*/*/libvelox.so"))
        self.assertEqual(len(libraries), 1)
        self.assertEqual(libraries[0].read_text(), "fresh backend\n")
        libraries[0].write_text("keep on failure\n")
        self.run_script("builddep-veloxbe-inc.sh", "--scala_version=2.12", success=False, FAIL_BUILD="1")
        self.assertEqual(libraries[0].read_text(), "keep on failure\n")
        self.assertTrue((self.build / "keep").exists())

    def test_incremental_keeps_dependency_options_and_cache_check(self):
        self.cache(BUILD_TESTS="ON", ENABLE_S3="OFF", ENABLE_GCS="OFF", ENABLE_ABFS="OFF")
        # The incremental entry only needs the existing CMake cache, not new graph metadata.
        (self.build / "build.ninja").unlink()
        calls = self.run_script("builddep-veloxbe-inc.sh", "--update_vcpkg")
        install = next(c for c in calls if c.startswith("vcpkg|"))
        self.assertEqual(install, "vcpkg|--build_tests=ON --enable_s3=OFF --enable_gcs=OFF --enable_abfs=OFF")
        self.cache(BUILD_TESTS="OFF", BUILD_BENCHMARKS="ON")
        calls = self.run_script("builddep-veloxbe-inc.sh", "--update_vcpkg")
        self.assertIn("--build_tests=OFF", next(c for c in calls if c.startswith("vcpkg|")))
        (self.build / "CMakeCache.txt").unlink()
        self.assertEqual(self.run_script("builddep-veloxbe-inc.sh", success=False), [])

    def test_bundle_keeps_native_before_maven(self):
        calls = self.run_script("package-vcpkg.sh", "--run_setup_script=OFF", "--spark_version=3.5")
        configure = next(c for c in calls if c.startswith("cmake|"))
        self.assertIn("-DBUILD_TESTS=ON", configure)
        self.assertIn("-DBUILD_BENCHMARKS=ON", configure)
        self.assertLess(next(i for i, c in enumerate(calls) if c.startswith("ninja|")),
                        next(i for i, c in enumerate(calls) if c.startswith("mvn|")))
        self.assertTrue(any(c.startswith("mvn|clean install -Pbackends-velox -Pspark-3.5") for c in calls))
        self.assertFalse(any("-Dcpp.build.dir=" in c for c in calls))

    def test_gpu_paths_follow_fetchcontent_directory(self):
        workflow = (ROOT / ".github/workflows/velox_backend_x86.yml").read_text()
        dockerfile = (ROOT / "dev/docker/cudf/Dockerfile.centos-9-jdk17-cuda12.9-cudf").read_text()
        self.assertIn("/work/cpp/build/_deps/curl-build/lib", workflow)
        self.assertIn("/opt/gluten/cpp/build/_deps/curl-build/lib", dockerfile)
        self.assertIn("cd cpp/build/_deps/cudf-build/", dockerfile)


if __name__ == "__main__":
    unittest.main()

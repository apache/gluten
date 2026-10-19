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
import re
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[3]


class NativeDependencyTest(unittest.TestCase):
    def run_command(self, *args):
        result = subprocess.run(args, text=True, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT)
        self.assertEqual(result.returncode, 0, result.stdout)
        return result.stdout

    def test_core_logging_links_after_velox_dependency_resolution(self):
        velox = Path(os.environ.get(
            "VELOX_HOME", ROOT / "ep/build-velox/build/velox_ep"
        )).resolve()
        core = (ROOT / "cpp/core/CMakeLists.txt").read_text()
        logging_link = re.search(
            r"target_link_libraries\(gluten_core_objects PRIVATE glog::glog[^)]*\)",
            core,
        ).group()
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory)
            (source / "velox").mkdir()
            (source / "velox/CMakeLists.txt").write_text(
                "set(CMAKE_FIND_PACKAGE_PREFER_CONFIG OFF)\n"
                "set(CMAKE_FIND_PACKAGE_TARGETS_GLOBAL ON)\n"
                f'list(PREPEND CMAKE_MODULE_PATH "{velox}/CMake")\n'
                "include(ResolveDependency)\n"
                "set(VELOX_DEPENDENCY_SOURCE SYSTEM)\n"
                "if(ENABLE_GLUTEN_VCPKG)\n"
                "  set(VELOX_GFLAGS_TYPE static)\n"
                "else()\n"
                "  set(VELOX_GFLAGS_TYPE shared)\n"
                "endif()\n"
                "velox_set_source(gflags)\n"
                "velox_resolve_dependency(gflags COMPONENTS ${VELOX_GFLAGS_TYPE})\n"
                "if(NOT TARGET gflags::gflags)\n"
                "  add_library(gflags::gflags ALIAS gflags)\n"
                "endif()\n"
                "velox_set_source(glog)\n"
                "velox_resolve_dependency(glog)\n"
            )
            (source / "CMakeLists.txt").write_text(
                "cmake_minimum_required(VERSION 3.28)\n"
                "project(core_logging LANGUAGES CXX)\n"
                "set(CMAKE_CXX_STANDARD 20)\n"
                "set(CMAKE_POSITION_INDEPENDENT_CODE ON)\n"
                "add_subdirectory(velox)\n"
                f'list(PREPEND CMAKE_MODULE_PATH "{ROOT}/cpp/CMake")\n'
                "find_package(glog REQUIRED)\n"
                "add_library(gluten_core_objects OBJECT logging.cc)\n"
                f"{logging_link}\n"
                "add_library(gluten SHARED)\n"
                "target_link_libraries(gluten PUBLIC gluten_core_objects)\n"
                "if(NOT APPLE)\n"
                "  target_link_options(gluten PRIVATE LINKER:--no-undefined)\n"
                "endif()\n"
                "add_executable(consumer main.cc)\n"
                "target_link_libraries(consumer PRIVATE gluten)\n"
            )
            (source / "logging.cc").write_text(
                '#include <glog/logging.h>\n#include <gflags/gflags.h>\n'
                'void log_message() { google::InitGoogleLogging("core_logging"); '
                'google::SetCommandLineOption("logtostderr", "1"); '
                'LOG(INFO) << "core logging works"; google::ShutdownGoogleLogging(); }\n'
            )
            (source / "main.cc").write_text(
                "void log_message(); int main() { log_message(); }\n"
            )
            build = source / "build"
            self.run_command("cmake", "-S", str(source), "-B", str(build), "-G", "Ninja")
            self.run_command("cmake", "--build", str(build), "--parallel", "2")
            self.assertIn("core logging works", self.run_command(str(build / "consumer")))

    def test_dependency_inputs_are_preserved_in_both_modes(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory)
            (source / "velox").mkdir()
            (source / "velox/CMakeLists.txt").write_text(
                "add_library(velox INTERFACE)\n"
                "if(NOT VELOX_DEPENDENCY_SOURCE STREQUAL \"AUTO\" OR\n"
                "   NOT VELOX_GFLAGS_TYPE STREQUAL \"shared\")\n"
                '  message(FATAL_ERROR "Dependency inputs were overridden")\n'
                "endif()\n"
                "foreach(name VELOX_DEPENDENCY_SOURCE Arrow_SOURCE simdjson_SOURCE)\n"
                '  if(NOT "$ENV{${name}}" STREQUAL "AUTO")\n'
                '    message(FATAL_ERROR "Dependency environment was overridden: ${name}")\n'
                "  endif()\n"
                "endforeach()\n"
            )
            for mode in ("ON", "OFF"):
                with self.subTest(vcpkg=mode):
                    (source / "CMakeLists.txt").write_text(
                        "cmake_minimum_required(VERSION 3.28)\n"
                        "project(dependency_inputs LANGUAGES NONE)\n"
                        f"set(ENABLE_GLUTEN_VCPKG {mode})\n"
                        "set(VELOX_DEPENDENCY_SOURCE AUTO)\n"
                        "set(VELOX_GFLAGS_TYPE shared)\n"
                        "foreach(name VELOX_DEPENDENCY_SOURCE Arrow_SOURCE simdjson_SOURCE)\n"
                        "  set(ENV{${name}} AUTO)\n"
                        "endforeach()\n"
                        'set(VELOX_HOME "${CMAKE_CURRENT_SOURCE_DIR}/velox")\n'
                        'set(VELOX_BUILD_PATH "${CMAKE_CURRENT_BINARY_DIR}/velox")\n'
                        f'add_subdirectory("{ROOT}/cpp/CMake/Velox" bridge)\n'
                        "get_target_property(aliased facebook::velox ALIASED_TARGET)\n"
                        'if(NOT aliased STREQUAL "velox")\n'
                        '  message(FATAL_ERROR "Incorrect Velox alias")\n'
                        "endif()\n"
                    )
                    self.run_command("cmake", "-S", str(source), "-B", str(source / mode))

    def test_gpu_compatibility_aliases(self):
        targets = (
            "velox_cudf_exec", "velox_cudf_vector", "velox_cudf_iceberg_connector",
            "velox_cudf_hive_connector", "velox_cudf_expression",
            "velox_cudf_expression_registry",
        )
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory)
            (source / "velox").mkdir()
            (source / "velox/CMakeLists.txt").write_text(
                "add_library(velox INTERFACE)\n"
                "if(VELOX_ENABLE_CUDF)\n"
                + "".join(f"  add_library({name} INTERFACE)\n" for name in targets)
                + "endif()\n"
            )
            for mode in ("ON", "OFF"):
                with self.subTest(gpu=mode):
                    checks = ""
                    for name in targets:
                        alias = f"facebook::velox::{name}"
                        if mode == "ON":
                            checks += (
                                f"get_target_property(aliased {alias} ALIASED_TARGET)\n"
                                f'if(NOT aliased STREQUAL "{name}")\n'
                                f'  message(FATAL_ERROR "Incorrect GPU alias: {alias}")\n'
                                "endif()\n"
                            )
                        else:
                            checks += (
                                f"if(TARGET {alias})\n"
                                f'  message(FATAL_ERROR "Unexpected GPU alias: {alias}")\n'
                                "endif()\n"
                            )
                    (source / "CMakeLists.txt").write_text(
                        "cmake_minimum_required(VERSION 3.28)\n"
                        "project(gpu_aliases LANGUAGES NONE)\n"
                        f"set(ENABLE_GPU {mode})\n"
                        'set(VELOX_HOME "${CMAKE_CURRENT_SOURCE_DIR}/velox")\n'
                        'set(VELOX_BUILD_PATH "${CMAKE_CURRENT_BINARY_DIR}/velox")\n'
                        f'add_subdirectory("{ROOT}/cpp/CMake/Velox" bridge)\n'
                        + checks
                    )
                    self.run_command("cmake", "-S", str(source), "-B", str(source / mode))

    def test_release_warning_is_limited_to_velox_decoder_test(self):
        cmake = (ROOT / "cpp/CMake/Velox/CMakeLists.txt").read_text()
        target = "velox_dwio_parquet_delta_bp_decoder_test"
        flag = "-Wno-error=free-nonheap-object"
        # Inspect the complete scope without configuring or compiling native code.
        expected = (
            'if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU"\n'
            '   AND CMAKE_CXX_COMPILER_VERSION VERSION_GREATER_EQUAL "13"\n'
            '   AND CMAKE_CXX_COMPILER_VERSION VERSION_LESS "14"\n'
            f'   AND TARGET {target})\n'
            "  target_compile_options(\n"
            f"    {target} PRIVATE\n"
            f'    "$<$<CONFIG:Release>:{flag}>")\n'
            "endif()"
        )
        self.assertIn(expected, cmake)
        self.assertEqual(cmake.count(flag), 1)
        self.assertGreater(cmake.index(expected), cmake.index('add_subdirectory("${VELOX_HOME}"'))

    def test_unknown_warning_option_is_clang_only(self):
        cmake = (ROOT / "cpp/CMakeLists.txt").read_text()
        warnings = cmake[cmake.index('set(KNOWN_WARNINGS'):
                         cmake.index('# see https://issues.apache.org/jira/browse/ARROW-4665')]
        with tempfile.TemporaryDirectory() as directory:
            script = Path(directory) / "warnings.cmake"
            for compiler in ("GNU", "Clang", "AppleClang"):
                with self.subTest(compiler=compiler):
                    condition = "" if compiler == "GNU" else "NOT "
                    script.write_text(
                        "cmake_minimum_required(VERSION 3.28)\n"
                        f"set(CMAKE_CXX_COMPILER_ID {compiler})\n"
                        "set(CMAKE_CXX_COMPILER_VERSION 13.3.0)\n"
                        f"{warnings}\n"
                        f'if({condition}KNOWN_WARNINGS MATCHES "-Wno-unknown-warning-option")\n'
                        '  message(FATAL_ERROR "Incorrect compiler warning scope")\n'
                        "endif()\n"
                    )
                    self.run_command("cmake", "-P", str(script))

    def test_prefix_includes_are_preserved_in_both_modes(self):
        cmake = (ROOT / "cpp/CMakeLists.txt").read_text()
        prefixes = cmake[cmake.index("set(GLUTEN_PREFIX_INCLUDE_DIRS)"):
                         cmake.index('include(ResolveDependency)')]
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory)
            (source / "prefix/include").mkdir(parents=True)
            script = source / "prefixes.cmake"
            for mode in ("ON", "OFF"):
                with self.subTest(vcpkg=mode):
                    script.write_text(
                        "cmake_minimum_required(VERSION 3.28)\n"
                        f"set(ENABLE_GLUTEN_VCPKG {mode})\n"
                        f'set(CMAKE_PREFIX_PATH "{source}/prefix")\n'
                        f"{prefixes}\n"
                        f'if(NOT "{source}/prefix/include" IN_LIST GLUTEN_PREFIX_INCLUDE_DIRS)\n'
                        '  message(FATAL_ERROR "Explicit include prefix was discarded")\n'
                        "endif()\n"
                    )
                    self.run_command("cmake", "-P", str(script))

    def test_missing_test_dependencies_use_fallback_in_both_modes(self):
        cmake = (ROOT / "cpp/CMakeLists.txt").read_text()
        dependencies = (
            ("benchmark", "BuildGoogleBenchmark",
             cmake[cmake.index("if(BUILD_TESTS OR BUILD_BENCHMARKS)"):
                   cmake.index("if(ENABLE_QAT)")]),
            ("GTest", "BuildGTest",
             cmake[cmake.index("if(BUILD_TESTS)\n"):
                   cmake.index("function(ADD_TEST_CASE")]),
        )
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory)
            for package, module, configuration in dependencies:
                (source / f"{module}.cmake").write_text(
                    f'message(STATUS "{package} fallback selected")\n'
                )
                for mode in ("ON", "OFF"):
                    with self.subTest(package=package, vcpkg=mode):
                        (source / "CMakeLists.txt").write_text(
                            "cmake_minimum_required(VERSION 3.28)\n"
                            "project(dependency_fallback LANGUAGES NONE)\n"
                            f"set(ENABLE_GLUTEN_VCPKG {mode})\n"
                            "set(BUILD_TESTS ON)\n"
                            "set(BUILD_BENCHMARKS ON)\n"
                            f"set(CMAKE_DISABLE_FIND_PACKAGE_{package} ON)\n"
                            'list(PREPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}")\n'
                            f"{configuration}\n"
                        )
                        output = self.run_command(
                            "cmake", "-S", str(source), "-B", str(source / f"{package}-{mode}"),
                            "-G", "Ninja"
                        )
                        self.assertIn(f"{package} fallback selected", output)


if __name__ == "__main__":
    unittest.main()

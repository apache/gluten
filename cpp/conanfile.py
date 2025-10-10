import os

from conan import ConanFile
from conan.tools import files, scm
from conan.tools.cmake import CMake, CMakeDeps, CMakeToolchain, cmake_layout
from conan.tools.env import Environment, VirtualBuildEnv, VirtualRunEnv

class GlutenConan(ConanFile):
    description = """Gluten Cpp"""

    name = "gluten"
    settings = "os", "arch", "compiler", "build_type"

    options = {
        "shared": [True, False],
        "fPIC": [True, False],
        "with_parquet" : [True, False],
        "enable_hdfs": [True, False],
        "enable_s3": [True, False],
        "enable_asan" : [True, False],
        "build_benchmarks": [True, False],
        "build_tests": [True, False],
        "build_examples": [True, False],
    }
    default_options = {
        "shared" : False,
        "fPIC" : True,
        "with_parquet": True,
        "enable_hdfs": True,
        "enable_s3": False,
        "enable_asan" : False,
        "build_benchmarks": False,
        "build_tests": False,
        "build_examples": False,
    }

    build_policy = "missing"

    scm_url = "https://github.com/apache/incubator-gluten.git"

    def source(self):
        git = scm.Git(self)

        # by default, use main branch
        git.clone(self.scm_url, target='.')

        # if use 'stable" channel, we should use a git relase tag.
        # Note: Conan 2.0 is no longer recommending to use variable users and channels
        #  refine this in future
        if self.channel and self.channel == 'stable':
            if not self.version:
                raise "Do specify a tag for a stable release."
            cmd = f"tags/{self.version} -b tag-{self.version}"
            git.checkout(cmd)
        else:
            scm_branch = self.version
            if scm_branch != 'main':
                cmd = f"-b {scm_branch} origin/{scm_branch}"
                git.checkout(cmd)

        self.folders.source = os.path.join(self.folders.source, 'cpp')

    def requirements(self):
        # TODO: to be removed user/channel in Conan 2.x
        user_channel = ""
        if hasattr(self, "user") and hasattr(self, "channel"):
            if self.user is not None and self.channel is not None:
                user_channel=f"@{self.user}/{self.channel}"
        bolt_version = os.getenv("BOLT_BUILD_VERSION", self.version)
        self.requires(f"bolt/{bolt_version}{user_channel}", transitive_headers=True, transitive_libs=True)

        protobuf_version = os.getenv("PROTOBUF_VERSION", "3.21.4")
        self.requires(f"protobuf/{protobuf_version}")
        if self.options.build_benchmarks or self.options.build_tests:
            self.requires("benchmark/[>=1.6.0]")
        self.requires("glog/0.7.1")
        self.requires("libbacktrace/cci.20210118")


    def build_requirements(self):
        self.tool_requires("protobuf/<host_version>")
        self.test_requires("gtest/1.17.0")
        self.test_requires("jemalloc/5.3.0")
        self.test_requires("duckdb/1.1.3")

    def layout(self):
        cmake_layout(self, build_folder='build')

    def config_options(self):
        pass

    def configure(self):
        postfix="/*"
        bolt = f"bolt{postfix}"
        self.options[bolt].spark_compatible = True
        self.options[bolt].enable_hdfs = self.options.enable_hdfs
        self.options[bolt].enable_s3 = self.options.enable_s3

        self.options[bolt].enable_asan = self.options.enable_asan

        if self.options.build_examples \
            or self.options.build_tests \
            or self.options.build_benchmarks:
            self.options[bolt].enable_test = True
            self.options[bolt].enable_testutil = True


    def generate(self):
        build_env = VirtualBuildEnv(self)
        build_env.generate()

        run_env = VirtualRunEnv(self)
        run_env.generate()

        tc = CMakeToolchain(self, generator="Ninja")
        tc.cache_variables["ENABLE_BOLT"] = True
        tc.cache_variables["BOLT_HOME"] = self.dependencies["bolt"].package_folder

        cxx_flags = ''
        if str(self.settings.arch) in ['x86', 'x86_64']:
            cxx_flags = f'{cxx_flags} -mno-avx512f '
            tc.cache_variables["CMAKE_CXX_FLAGS"] = cxx_flags
            tc.cache_variables["CMAKE_C_FLAGS"] = cxx_flags

        # To avoid R_AARCH64_CALL26 link error when binary file exceeds 127M
        # -mcmodel=large if -fPIC removed
        if str(self.settings.arch) in ['armv8', 'armv9']:
            cxx_flags = f'{cxx_flags}  -ffunction-sections -fdata-sections '

            # Support CRC & NEON on ARMv8
            if str(self.settings.arch) in ['armv8']:
                cxx_flags = f'{cxx_flags} -march=armv8.3-a'
            if str(self.settings.arch) in ['armv9']:
                cxx_flags = f'{cxx_flags} -march=-march=armv9-a'

            tc.cache_variables["CMAKE_CXX_FLAGS"] = cxx_flags
            tc.cache_variables["CMAKE_C_FLAGS"] = cxx_flags

        tc.cache_variables["BOLT_ENABLE_PARQUET"] = self.options.with_parquet

        tc.cache_variables["ENABLE_HDFS"] = "ON" if self.options.enable_hdfs else "OFF"
        tc.cache_variables["ENABLE_S3"] = "ON" if self.options.enable_s3 else "OFF"

        tc.cache_variables["BUILD_BENCHMARKS"] = self.options.build_benchmarks
        tc.cache_variables["BUILD_TESTS"] = self.options.build_tests
        tc.cache_variables["BUILD_EXAMPLES"] = self.options.build_examples
        if self.options.build_benchmarks:
            tc.cache_variables["ENABLE_ORC"] = False
        if self.options.enable_asan:
            cxx_flags = f'{cxx_flags} -fsanitize=address -fno-omit-frame-pointer '
            tc.cache_variables["CMAKE_CXX_FLAGS"] = cxx_flags
            tc.cache_variables["CMAKE_C_FLAGS"] = cxx_flags

        tc.generate()

        deps = CMakeDeps(self)
        deps.set_property("flex", "cmake_find_mode", "config")
        deps.generate()

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()

    def package(self):
        files.copy(self, "LICENSE", src=self.source_folder, dst=os.path.join(self.package_folder, "licenses"))
        files.copy(self, "CONTRIBUTING.md", src=self.source_folder, dst=os.path.join(self.package_folder, "licenses"))
        files.copy(self, "README.md", src=self.source_folder, dst=os.path.join(self.package_folder, "licenses"))

        cmake = CMake(self)
        cmake.configure()
        cmake.install()

    def package_info(self):
        self.cpp_info.set_property("cmake_file_name", "gluten")
        self.cpp_info.set_property("cmake_find_mode", "both")
        self.cpp_info.set_property("cmake_target_name", "gluten::gluten")

        self.cpp_info.includedirs = ['include']
        self.cpp_info.libs = ["bolt_backend"]

        self.cpp_info.components["libgluten"].libs = ["gluten"]
        self.cpp_info.components["libgluten"].requires.append("bolt::bolt")

        self.cpp_info.components["bolt_backend"].libs = ["bolt_backend"]
        self.cpp_info.components["bolt_backend"].requires.append("libgluten")
        self.cpp_info.components["bolt_backend"].requires.append("bolt::bolt")

        self.cpp_info.requires.extend(["glog::glog", "bolt::bolt"])

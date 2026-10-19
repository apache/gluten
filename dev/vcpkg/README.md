# Build Gluten + Velox in Vcpkg Environment

## Overview

Currently, the `builtin-baseline` set in `vcpkg.json` is the commit hash for the `2026.03.18` tag of vcpkg.
The versions of all dependency libraries are determined by their respective ports at this vcpkg version,
except for those overridden in `vcpkg.json`, `vcpkg-configuration.json`, and overlay ports.

## Build in docker

For main branch code, you can follow the commands below.

- Pull the docker image: `docker pull apache/gluten:vcpkg-almalinux-8-gcc13`
- Build native code: `bash dev/ci-velox-buildstatic-centos-8.sh`
- Build JVM code: `./build/mvn clean install -Pbackends-velox -Pspark-3.5 -DskipTests`

The gluten packages will be placed in `$GLUTEN_REPO/package/target/gluten-velox-bundle-*.jar`.

## Setup build environment manually

### Install native dependencies only

From the Gluten checkout, install dependencies without compiling Gluten or
Velox and without running Maven:

```sh
source dev/vcpkg/env.sh --build_tests=ON --enable_s3=ON --enable_gcs=ON --enable_hdfs=ON
```

After installing dependencies, prepare the environment in each new shell before
configuring. From the Gluten checkout, reuse the installed dependencies with:

```bash
source dev/vcpkg/env.sh --skip-install
```

Alternatively, set the three environment variables read by the toolchain:

```bash
export VCPKG_ROOT="$PWD/dev/vcpkg/.vcpkg"
export VCPKG_TRIPLET=x64-linux-avx
export VCPKG_MANIFEST_DIR="$PWD/dev/vcpkg"
```

Use `arm64-linux-neon` for ARM64. Select Gluten's toolchain with
`-DCMAKE_TOOLCHAIN_FILE=$PWD/dev/vcpkg/toolchain.cmake`. The toolchain reads these
three values from the environment, not from same-named `-D` options; it does not
infer missing paths or a triplet. `VCPKG_MANIFEST_DIR` must contain the existing
`vcpkg_installed` directory. Manual exports only provide the toolchain inputs;
`env.sh` also prepares tool paths and pkg-config settings. Neither method above
installs dependencies. For a non-vcpkg build, use a fresh build directory without
this toolchain or its environment variables.

### Unified native CMake graph

Open `cpp/CMakeLists.txt` as the native project. Velox is added from
`VELOX_HOME`; its CMake files are not modified. The real `velox` target builds
the static Velox library, while `gluten_velox_backend` produces `libvelox.so`.
Use `facebook::velox` and `gluten::velox_backend` as namespaced aliases when
linking these libraries. Build commands still use the real targets `velox` and
`gluten_velox_backend`; aliases do not create build targets.
`gluten` continues to produce `libgluten.so`. Native tests and benchmarks link
the implementation object targets instead of these JNI shared libraries.
Gluten's memory test target is named `gluten_velox_memory_test` to distinguish
it from the upstream `velox_memory_test`.

The unified graph does not select a dependency provider. Gluten's vcpkg
toolchain defaults Velox to installed dependencies and static gflags; explicit
CMake settings can override these defaults. Without that toolchain, Velox
retains its normal dependency resolution defaults.

Direct CMake builds enable Gluten tests and benchmarks by default; Velox's
own tests and benchmarks are disabled by default. Configure with `BUILD_TESTS` /
`BUILD_BENCHMARKS` for Gluten and `VELOX_BUILD_TESTING` /
`VELOX_ENABLE_BENCHMARKS` for Velox to override these defaults. These options
are independent; the native library and required test utilities are built in
the same graph.

The default `dev/builddeps-veloxbe.sh` flow configures and builds this unified
graph without a standalone Velox build first. It recreates `cpp/build`, relying
on ccache for compilation reuse, and sets all four test/benchmark options from `--build_tests`,
`--build_benchmarks`, `--build_velox_tests`, and `--build_velox_benchmarks`
(each defaults to `OFF`); pass the flags again to keep them enabled.
Before using `dev/builddep-veloxbe-inc.sh`, configure the unified graph through
the main script with the required flags. The incremental script builds only
`gluten_velox_backend` and its dependencies, not test/benchmark targets.

To check IDE target visibility, request the CMake File API:

```sh
mkdir -p cpp/build/.cmake/api/v1/query
touch cpp/build/.cmake/api/v1/query/codemodel-v2
# Re-run your CMake configure command with all four test/benchmark options ON.
python3 cpp/CMake/tests/check_unified_graph.py cpp/build
```

Velox's GEO support follows its upstream default (ON) or an explicit
`-DVELOX_ENABLE_GEO=OFF`; tests and benchmarks do not override this setting.
GEOS and S2 remain vcpkg-managed dependencies.

### CLion without presets

Open the Gluten `cpp` directory as a CMake project. No overlay, copied Velox
CMake files, or `CMakePresets.json` is needed. Install dependencies first as
above, then set `CMAKE_TOOLCHAIN_FILE` in the profile's CMake options when using
vcpkg. Set `VCPKG_ROOT`, `VCPKG_TRIPLET`, and `VCPKG_MANIFEST_DIR` in the profile's
environment, or launch CLion from a shell prepared as above. The project does not
implicitly select a toolchain.

In Settings > Build, Execution, Deployment > CMake, create ordinary profiles:

| Profile | Build type | Build directory |
| --- | --- | --- |
| Debug | Debug | `$GLUTEN_HOME/.clion-build/debug` |
| Release | Release | `$GLUTEN_HOME/.clion-build/release` |

Use absolute paths in CLion and select Ninja as the generator. No extra CMake
options are needed for Gluten tests and benchmarks; Velox tests and benchmarks
remain off unless explicitly enabled. Build the `gluten_velox_backend` target
to produce both Java JNI libraries. Since Velox is a separate checkout outside
`cpp`, CLion may show it under **External Sources**; this is expected and code
navigation works. Copying or symlinking Velox into the project would add
complexity without improving the CMake build graph.

Use the container's C/C++ compiler, CMake 3.28 or newer, Ninja, and installed
JDK. Library/header navigation follows CMake usage requirements; vcpkg binary
packages do not necessarily include third-party implementation source files.
Do not reuse a standalone Velox cache for this unified project.

### Runtime validation notes

Run Velox tests through CTest, which preserves upstream per-test process
isolation. Running an entire test executable can expose shared-state conflicts
between otherwise independent cases.
Use `ctest` from the same CMake installation that configured the build.
After sourcing `dev/vcpkg/env.sh`, keep that environment for test execution;
an older system CTest may not understand the generated GoogleTest policies.

On WSL, a piped kernel crash collector can hang GoogleTest death-test children.
If this occurs, suppress the collector for the test process only, without
changing the host's kernel configuration:

```sh
prlimit --core=1:1 ctest --test-dir .clion-build/debug --output-on-failure
```

### Setup build toolkits

Please install build depends on your system to compile all libraries:

``` sh
sudo $GLUTEN_REPO/dev/vcpkg/setup-build-depends.sh
```

GCC 12 is the minimum required compiler. It needs to be enabled beforehand.

For unsupported linux distro, you can install the following packages from package manager.

* zip
* tar
* wget
* curl
* git >= 2.7.4
* gcc >= 12
* pkg-config
* autotools
* flex >= 2.6.0
* bison
* openjdk 8
* maven

### Build gluten + velox with vcpkg installed dependencies

With `--enable_vcpkg=ON`, the below script will install all static libraries into `./vcpkg_installed/`. And it will
also set `$PATH` and `$CMAKE_TOOLCHAIN_FILE` to make CMake to locate the binary tools and libraries.
You can configure [binary cache](https://learn.microsoft.com/en-us/vcpkg/users/binarycaching) to accelerate the build.

``` sh
$GLUTEN_REPO/dev/buildbundle-veloxbe.sh --enable_vcpkg=ON --build_tests=ON --build_benchmarks=ON --enable_s3=ON  --enable_hdfs=ON
```

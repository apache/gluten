---
layout: page
title: Dev Container
nav_order: 18
parent: Developer Overview
---

# Develop Gluten in a Dev Container

Gluten ships a [Dev Container](https://containers.dev/) configuration at
[`.devcontainer/devcontainer.json`](https://github.com/apache/gluten/blob/main/.devcontainer/devcontainer.json),
so you can develop inside a pre-built image that already has the JDK, Maven, GCC
toolset and the whole Velox native dependency stack installed.

## Prerequisites

[Docker](https://docs.docker.com/get-docker/) plus
[VS Code](https://code.visualstudio.com/) with the
[Dev Containers extension](https://code.visualstudio.com/docs/devcontainers/containers),
or a [Codespaces](https://docs.github.com/en/codespaces)-enabled account.

## Open the workspace

In VS Code, run **Dev Containers: Reopen in Container** from the Command Palette
(`F1`). In Codespaces, create a Codespace for the repository.

## What the configuration does

It opens the workspace in `apache/gluten:centos-9-jdk8` (CentOS Stream 9, JDK 8,
**dynamically linked** dependencies), puts GCC 12 on `PATH` through `remoteEnv`
because the default GCC 11 cannot compile Velox's C++20 sources, and runs
[`.devcontainer/post-create.sh`](https://github.com/apache/gluten/blob/main/.devcontainer/post-create.sh),
which installs JDK 17, `clang-format` 15 and the `regex` module, sizes `NUM_THREADS`
for the machine and prints the build commands.

**The native build is not run automatically.** It takes tens of minutes to several
hours, which would stall container creation and leave a half-built tree behind
whenever the editor disconnects or a Codespace times out.

## Build Gluten

```bash
./dev/buildbundle-veloxbe.sh --run_setup_script=OFF --build_arrow=OFF \
                             --build_tests=ON --spark_version=3.5
```

| Flag | Why |
|---|---|
| `--run_setup_script=OFF` | Velox's third-party libraries are already installed in the image; `ON` rebuilds them all from source into `/usr/local`. |
| `--build_arrow=OFF` | Arrow is already installed under `/usr/local` and its jars are in `~/.m2`. |
| `--build_tests=ON` | Also builds the C++ unit tests. Drop it if you only need the jars. |
| `--spark_version=3.5` | The default, `ALL`, runs five full Maven builds (Spark 3.3 to 4.1). |

To rebuild only the native side after a C++ change:

```bash
./dev/builddeps-veloxbe.sh --run_setup_script=OFF --build_arrow=OFF \
                           --build_tests=ON build_velox build_gluten_cpp
```

Drop `build_velox` when only Gluten's own C++ under `cpp/` changed. Keep
`--build_tests` matched with the flag you built with: `build_gluten_cpp` wipes
`cpp/build` on every run, so omitting it silently drops the C++ test binaries.

Spark 4.0/4.1 need JDK 17 and Scala 2.13. `buildbundle-veloxbe.sh` adds the Maven
profiles, but `JAVA_HOME` is yours to set:

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk
./dev/buildbundle-veloxbe.sh --run_setup_script=OFF --build_arrow=OFF --spark_version=4.0
```

### Build parallelism and the OOM killer

`builddeps-veloxbe.sh` sizes `NUM_THREADS` as `nproc --ignore=2`, ignoring memory,
while Velox's heavier translation units peak at ~3.5 GB resident each. On a 32-core,
62 GB container that is 30 jobs asking for roughly 100 GB, and the OOM killer takes
down the build or the container. `post-create.sh` therefore exports a value allowing
~4 GB per job — 13 jobs on that machine, measured at a 41 GB peak.

An explicit `export NUM_THREADS=<n>` still wins. VS Code tasks do not read
`~/.bashrc`, so pass `--num_threads=<n>` there.

## Run the tests

The image unpacks a Spark distribution for every supported version under `/opt/shims`,
which is what `spark.test.home` needs. CI runs the Spark 3.3/3.4/3.5 unit tests on
JDK 17:

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk
./build/mvn test -Pspark-ut -Pbackends-velox -Pspark-3.5 -Pjava-17 \
    -DargLine="-Dspark.test.home=/opt/shims/spark35/spark_home/" \
    -DwildcardSuites=org.apache.spark.sql.GlutenSQLQuerySuite
```

`-DwildcardSuites` takes a fully qualified class name and keeps a run to minutes;
without it the whole suite runs for hours. Do not add `-pl gluten-ut`: it is an
aggregator POM, `-pl` does not pull in its children, and the build finishes in seconds
having run nothing. See [HowTo](./HowTo.md#3-how-to-debug-javascala) for more.

C++ unit tests (requires `--build_tests=ON`):

```bash
cd cpp/build && ctest -V
```

## Static or dynamic link?

"Dynamic" and "static" describe **how third-party dependencies are linked**. Velox
itself is archived into `libvelox.a` and linked into Gluten's shared libraries either
way, so `VELOX_BUILD_SHARED` only controls whether folly, gflags and glog are built
shared; `devcontainer.json` sets it to `ON` to match the image.

| | Dynamic (`--enable_vcpkg=OFF`, the default) | Static (`--enable_vcpkg=ON`) |
|---|---|---|
| Image | `apache/gluten:centos-9-jdk8` | `apache/gluten:vcpkg-centos-9` |
| Dependencies | Pre-installed system libraries | vcpkg ports |
| Spark distributions | `/opt/shims`, so `gluten-ut` runs | Not installed |
| JDK | 8, with 17 added by `post-create.sh` | 17 only |
| Resulting jar | Needs the same shared libraries | Runs on any x86-64 Linux |

Static linking exists to produce **portable release jars**, which is worth paying for
in CI and releases but buys a developer nothing, since the jar never leaves the
container. The vcpkg image also lacks `/opt/shims`, so the Spark unit tests cannot
run in it at all.

vcpkg itself caches well: `apache/gluten:vcpkg-centos-9` bakes in
`VCPKG_BINARY_SOURCES=clear;files,/var/cache/vcpkg,readwrite`, and because the ABI
hash is computed per port, a checkout that has moved rebuilds only the ports it
actually changed plus their dependents. Be aware that the ABI hash also covers the
toolchain, so a change of compiler or triplet invalidates every port at once, and
vcpkg does not explain why it started rebuilding.

To reproduce a static-link problem, switch `image` to `apache/gluten:vcpkg-centos-9`
and build with `./dev/ci-velox-buildstatic-centos-9.sh` (it exports `NUM_THREADS=2`
for CI runners; raise it first). Build trees are tied to the image that produced
them — `cpp/build/CMakeCache.txt` records the vcpkg toolchain and
`ep/build-velox/build/velox_ep/_build/` records `VELOX_BUILD_SHARED` — so wipe them
when switching:

```bash
rm -rf cpp/build ep/build-velox/build/velox_ep/_build ep/_ep \
       dev/vcpkg/.vcpkg dev/vcpkg/vcpkg_installed
```

## Machine sizing

`hostRequirements` asks for 4 CPUs, 16 GB of memory and 64 GB of storage. Storage is
the binding constraint: the image plus the Velox build tree does not fit the 32 GB
disk of the smaller Codespaces machine types, so those are marked "Below dev container
requirements". Only Codespaces honours `hostRequirements`; other runtimes ignore it,
so size the Docker VM yourself.

## Use another image

Other pre-built images are published to
[Docker Hub](https://hub.docker.com/r/apache/gluten/tags); their Dockerfiles live in
[`dev/docker/`](https://github.com/apache/gluten/tree/main/dev/docker) and are
described in [Velox Backend CI](./velox-backend-CI.md#docker-build).
`apache/gluten:centos-9-jdk17` is the same dynamic-link image with JDK 17 as the
default; `centos-8-*` variants build against an older glibc. To switch, edit the
`image` field in `.devcontainer/devcontainer.json`.

To use these images outside a Dev Container, see
[Build Gluten Velox backend in docker](./velox-backend-build-in-docker.md).

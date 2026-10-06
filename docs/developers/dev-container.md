---
layout: page
title: Dev Containers
nav_order: 18
parent: Developer Overview
---

# Develop Gluten in a Dev Container

Gluten provides Dev Container configurations for daily Velox development and
static-link packaging. Both use pre-built images with the native dependency
stack installed, and neither builds Gluten during container creation.

## Prerequisites

[Docker](https://docs.docker.com/get-docker/) plus
[VS Code](https://code.visualstudio.com/) with the
[Dev Containers extension](https://code.visualstudio.com/docs/devcontainers/containers),
or a [Codespaces](https://docs.github.com/en/codespaces)-enabled account.

## Choose a configuration

| Configuration | Path | Use case | Spark unit tests |
|---|---|---|---|
| **Velox dynamic link** (default) | `.devcontainer/devcontainer.json` | Daily development | Yes |
| **Velox static link** | `.devcontainer/velox-static/devcontainer.json` | Portable jars and vcpkg issue reproduction | No (`/opt/shims` is not installed) |

In VS Code, run **Dev Containers: Reopen in Container** from the Command
Palette (`F1`) and select a configuration. In Codespaces, open **Create
codespace with options** and select the configuration before creating the
codespace.

The shared
[post-create script](https://github.com/apache/gluten/blob/main/.devcontainer/post-create.sh)
installs missing development tools, sizes `NUM_THREADS` for the machine and
prints commands for the selected configuration.

**The native build is not run automatically.** It takes tens of minutes to
several hours, which would stall container creation and leave a half-built tree
behind if the editor disconnects or a Codespace times out.

## Velox dynamic-link development

The default configuration uses `apache/gluten:centos-9-jdk8`. It includes
dynamically linked dependencies, Arrow under `/usr/local`, a pre-warmed Maven
repository and Spark distributions under `/opt/shims`.

Build the native backend and the Spark 3.5 jars:

```bash
./dev/buildbundle-veloxbe.sh --run_setup_script=OFF --build_arrow=OFF \
                             --build_tests=ON --spark_version=3.5
```

| Flag | Why |
|---|---|
| `--run_setup_script=OFF` | Velox's third-party libraries are already installed; `ON` rebuilds them from source. |
| `--build_arrow=OFF` | Arrow is already installed under `/usr/local` and its jars are in `~/.m2`. |
| `--build_tests=ON` | Also builds the C++ unit tests. Drop it if you only need the jars. |
| `--spark_version=3.5` | The default, `ALL`, runs four Maven builds (Spark 3.4 to 4.1). |

### Build for Spark 4.1

Spark 4.0/4.1 require JDK 17 and Scala 2.13. The dynamic image defaults to JDK
8, while `post-create.sh` installs JDK 17 alongside it. Switch the running JDK
before starting the build:

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk
export PATH="$JAVA_HOME/bin:$PATH"
java -version  # must report 17

./dev/buildbundle-veloxbe.sh --run_setup_script=OFF --build_arrow=OFF \
                             --build_tests=ON --spark_version=4.1
```

`buildbundle-veloxbe.sh` adds `-Pjava-17`, `-Pscala-2.13` and the Java 17
release target for Spark 4.x, but Maven profiles cannot switch the JDK that is
already running. If JDK 8 remains active, Scala fails with:

```text
scalac error: '17' is not a valid choice for '-release'
```

### Rebuild native code

After changing C++ code:

```bash
./dev/builddeps-veloxbe.sh --run_setup_script=OFF --build_arrow=OFF \
                           --build_tests=ON build_velox build_gluten_cpp
```

Drop `build_velox` when only Gluten's own C++ under `cpp/` changed. Keep
`--build_tests` matched with the original build: `build_gluten_cpp` wipes
`cpp/build`, so omitting it also removes the C++ test binaries.

### Run tests

Run a Spark 3.5 suite on JDK 17:

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk
export PATH="$JAVA_HOME/bin:$PATH"

./build/mvn test -Pspark-ut -Pbackends-velox -Pspark-3.5 -Pjava-17 \
    -DargLine="-Dspark.test.home=/opt/shims/spark35/spark_home/" \
    -DwildcardSuites=org.apache.spark.sql.GlutenSQLQuerySuite
```

`-DwildcardSuites` takes a fully qualified class name and keeps a run to
minutes. Do not add `-pl gluten-ut`: it selects only the aggregator POM and no
test suite runs. See [HowTo](./HowTo.md#3-how-to-debug-javascala) for more.

Run the C++ unit tests after building with `--build_tests=ON`:

```bash
cd cpp/build && ctest -V
```

## Velox static-link packaging

The static configuration uses `apache/gluten:vcpkg-centos-9`. It opens without
building Gluten and persists the image's vcpkg binary cache across container
rebuilds.

Build a portable Spark 3.5 jar:

```bash
./dev/buildbundle-veloxbe.sh --enable_vcpkg=ON --build_arrow=OFF \
                             --spark_version=3.5
```

S3, GCS, HDFS and ABFS are disabled by default. Enable only what the jar needs:

```bash
./dev/buildbundle-veloxbe.sh --enable_vcpkg=ON --build_arrow=OFF \
    --spark_version=3.5 --enable_s3=ON
```

Each enabled feature may restore or build additional vcpkg ports. vcpkg caches
ports by ABI hash; changing the compiler, triplet or relevant port inputs can
invalidate that cache. Native test binaries are disabled in this packaging
workflow.

The static image uses JDK 17 by default. For Spark 4.1:

```bash
java -version  # must report 17

./dev/buildbundle-veloxbe.sh --enable_vcpkg=ON --build_arrow=OFF \
                             --spark_version=4.1
```

The static image does not include `/opt/shims`; use the dynamic configuration
for Spark unit tests. On arm64, `post-create.sh` also selects the arm64 vcpkg
triplet and enables the required system binaries automatically.

## Build parallelism

`builddeps-veloxbe.sh` defaults `NUM_THREADS` to `nproc --ignore=2`, which
ignores memory. Velox's heavier translation units use about 3.5 GB each, so a
core-rich machine can invoke the OOM killer. `post-create.sh` exports a value
allowing about 4 GB per job and recomputes it whenever a shell opens, so it also
follows a resized Codespace.

An explicit `export NUM_THREADS=<n>` still wins. VS Code tasks do not read
`~/.bashrc`, so pass `--num_threads=<n>` in a task.

## Switch between configurations

Static and dynamic build trees are not interchangeable:
`cpp/build/CMakeCache.txt` records the vcpkg toolchain, and
`ep/build-velox/build/velox_ep/_build/` records dependency linkage. Remove the
image-specific state after switching:

```bash
rm -rf cpp/build ep/build-velox/build/velox_ep/_build ep/_ep \
       dev/vcpkg/.vcpkg dev/vcpkg/vcpkg_installed
```

The configurations use separate ccache, Maven and vcpkg-cache volumes, so
rebuilding one environment does not contaminate the other.

## Machine sizing

The dynamic configuration requests 4 CPUs, 16 GB of memory and 64 GB of
storage. The static configuration requests 8 CPUs, 64 GB of memory and 64 GB
of storage because static linking needs more memory.

Only Codespaces honors `hostRequirements`; for local development, size the
Docker VM yourself.

## Images

Published images are available on
[Docker Hub](https://hub.docker.com/r/apache/gluten/tags); their Dockerfiles
live in [`dev/docker/`](https://github.com/apache/gluten/tree/main/dev/docker)
and are described in
[Velox Backend CI](./velox-backend-CI.md#docker-build).

To use the images outside a Dev Container, see
[Build Gluten Velox backend in docker](./velox-backend-build-in-docker.md).

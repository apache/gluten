---
layout: page
title: Pre-built Docker Images
nav_order: 22
parent: Developer Overview
---
# Pre-built Docker Images

Gluten publishes pre-built images to Docker Hub as
[`apache/gluten`](https://hub.docker.com/r/apache/gluten/tags). CI runs its builds and tests in
them, and you can use them as local development environments too. Each image comes from a
Dockerfile under [`dev/docker/`](https://github.com/apache/gluten/tree/main/dev/docker), built by the
[`docker_image.yml`](https://github.com/apache/gluten/blob/main/.github/workflows/docker_image.yml)
workflow.

## Images

Sizes are the compressed size of a single architecture as shown on Docker Hub (2026-10-18 build),
except where marked. On disk, a pulled image is roughly twice as large.

| Tag | Dockerfile | Arch | Size | Used by |
|---|---|---|---|---|
| [`vcpkg-almalinux-8-gcc13`](https://hub.docker.com/r/apache/gluten/tags?name=vcpkg-almalinux-8-gcc13) | [`Dockerfile.almalinux8-gcc13-static-build`](https://github.com/apache/gluten/blob/main/dev/docker/Dockerfile.almalinux8-gcc13-static-build) | amd64, arm64 | 1.7 GB | Static native builds in `velox_backend_x86`, `delta_spark_ut`, `velox_kafka`, `velox_native_build` (Iceberg), `velox_backend_ansi`, `velox_backend_enhanced`, `velox_nightly`, `velox_backend_cache`, `build_bundle_package` and `build_release`; [release builds](../../../developers/HowToRelease.md); [static build in Docker](velox-backend-build-in-docker.md) |
| [`vcpkg-centos-9`](https://hub.docker.com/r/apache/gluten/tags?name=vcpkg-centos-9) | [`Dockerfile.centos9-static-build`](https://github.com/apache/gluten/blob/main/dev/docker/Dockerfile.centos9-static-build) | amd64, arm64 | 2.1 GB | Static native build in `velox_backend_arm`; `velox_backend_cache`; the static [dev container](../../../developers/dev-container.md) |
| [`centos-8-jdk8`](https://hub.docker.com/r/apache/gluten/tags?name=centos-8-jdk8) | [`Dockerfile.centos8-dynamic-build`](https://github.com/apache/gluten/blob/main/dev/docker/Dockerfile.centos8-dynamic-build) | amd64 | 5.5 GB | Spark 3.4 / 3.5 unit tests and the Uniffle TPC job in `velox_backend_x86`; `velox_backend_enhanced`; `velox_nightly`; [dynamic build in Docker](velox-backend-build-in-docker.md) |
| [`centos-8-jdk17`](https://hub.docker.com/r/apache/gluten/tags?name=centos-8-jdk17) | same, `JAVA_VERSION=17` | amd64 | 5.5 GB | Spark 4.0 unit tests and build checks in `velox_backend_x86`; `flink` |
| [`centos-9-jdk8`](https://hub.docker.com/r/apache/gluten/tags?name=centos-9-jdk8) | [`Dockerfile.centos9-dynamic-build`](https://github.com/apache/gluten/blob/main/dev/docker/Dockerfile.centos9-dynamic-build) | amd64, arm64 | 5.1 GB | `cpp-test-udf-test` (shared build, C++ tests, clang-tidy) in `velox_backend_x86` and `velox_backend_arm`; `velox_backend_cache`; the default [dev container](../../../developers/dev-container.md) |
| [`centos-9-jdk17`](https://hub.docker.com/r/apache/gluten/tags?name=centos-9-jdk17) | same, `JAVA_VERSION=17` | amd64 | 5.1 GB | Spark 4.1 / 4.2 unit tests in `velox_backend_x86`; `delta_spark_ut`; `velox_backend_ansi`; the Iceberg test shards (`iceberg_upstream_revision`) |
| [`centos-9-jdk17-cuda13.1-cudf`](https://hub.docker.com/r/apache/gluten/tags?name=centos-9-jdk17-cuda13.1-cudf) | [`cudf/Dockerfile.centos-9-jdk17-cuda13.1-cudf`](https://github.com/apache/gluten/blob/main/dev/docker/cudf/Dockerfile.centos-9-jdk17-cuda13.1-cudf) | amd64 | 13.2 GB | `build-cudf-centos-9` in `velox_backend_x86`; `velox_backend_cache`; [GPU support](../get-started/VeloxGPU.md) |
| [`centos-8-runtime`](https://hub.docker.com/r/apache/gluten/tags?name=centos-8-runtime) | [`Dockerfile.centos8-runtime`](https://github.com/apache/gluten/blob/main/dev/docker/Dockerfile.centos8-runtime) | amd64 | 1.9 GB\* | `tpc-test-centos8` and the Celeborn TPC job in `velox_backend_x86` |
| [`centos-9-runtime`](https://hub.docker.com/r/apache/gluten/tags?name=centos-9-runtime) | [`Dockerfile.centos9-runtime`](https://github.com/apache/gluten/blob/main/dev/docker/Dockerfile.centos9-runtime) | arm64 | 1.8 GB\* | `tpc-test-centos8` in `velox_backend_arm` |
| [`ubuntu-22.04-runtime`](https://hub.docker.com/r/apache/gluten/tags?name=ubuntu-22.04-runtime) | [`ubuntu/Dockerfile.ubuntu22-runtime`](https://github.com/apache/gluten/blob/main/dev/docker/ubuntu/Dockerfile.ubuntu22-runtime) | amd64 | 3.4 GB\* | `tpc-test-ubuntu` in `velox_backend_x86` |

\* Uncompressed size from a local build. Docker Hub has no size for these yet because they have not
been published.

To use an image, pull it with `docker pull apache/gluten:<tag>`:

```bash
docker pull apache/gluten:vcpkg-almalinux-8-gcc13
docker run -it --rm -v "$PWD":/work -w /work apache/gluten:vcpkg-almalinux-8-gcc13 bash
```

For a multi-arch tag, Docker pulls the variant that matches your machine.

## What the images contain

The build images clone Gluten to fill in the dependency caches, then remove the clone and the
ccache. None of them contains Gluten source or binaries.

The tools that CI jobs need at runtime are installed in the images, so jobs don't have to install
packages or reach a package mirror. `dev/docker/install-ci-tools.sh` adds `gh` and `jq` (used by the
Apache Stash cache actions) to every image except the GPU image.

### Static build images (`vcpkg-*`)

These images build `libvelox.so` and `libgluten.so` with all dependencies statically linked through
vcpkg, so the libraries run on most Linux distributions (see
[static link](velox-backend-build-in-docker.md)).

| Tag | Base | Compiler | `JAVA_HOME` |
|---|---|---|---|
| `vcpkg-almalinux-8-gcc13` | `almalinux:8` (glibc 2.28) | gcc-toolset-13 | JDK 8 |
| `vcpkg-centos-9` | `quay.io/centos/centos:stream9` | gcc-toolset-12 and 13 | JDK 17 |

Both contain:
- A vcpkg binary cache in `/var/cache/vcpkg`, with the binary packages for every port in
  `dev/vcpkg`. `dev/vcpkg/populate-static-image-cache.sh` builds them with S3, GCS, HDFS, ABFS and
  tests enabled. `VCPKG_BINARY_SOURCES` points at the cache, so `--enable_vcpkg=ON` builds restore
  dependencies rather than compile them. Set `VCPKG_BINARY_SOURCES=clear` to turn this off.
- A Maven repository in `/root/.m2`, filled by
  `mvn dependency:go-offline -Pbackends-velox -Piceberg -Pdelta -Pspark-3.5`.
- `gh` and `jq`. `vcpkg-almalinux-8-gcc13` also has tzdata and Python 3.9 as `python3`.

### Dynamic build and test images (`centos-8-jdk*`, `centos-9-jdk*`)

These images build against shared system libraries. The Spark unit tests run in them.

| Tag | Base | Compiler | `JAVA_HOME` |
|---|---|---|---|
| `centos-8-jdk8` / `-jdk17` | `centos:8` (EOL, repos point at `vault.centos.org`) | gcc-toolset-12 | JDK 8 / 17 |
| `centos-9-jdk8` / `-jdk17` | `quay.io/centos/centos:stream9` | gcc-toolset-12 and 13 | JDK 8 / 17 |

They contain:
- Arrow, installed into the system paths by `builddeps-veloxbe.sh build_arrow`.
- Spark test resources for Spark 3.4, 3.5, 3.5 with Scala 2.13, 4.0, 4.1 and 4.2, in
  `/opt/shims/spark<version>/spark_home` (from `.github/workflows/util/install-spark-resources.sh`).
  The unit tests set `spark.test.home` to these directories.
- Release tarballs in `/opt`: Celeborn 0.5.4 and 0.6.3. The CentOS 8 images also have Uniffle
  0.10.0 and Hadoop 2.10.2.
- A Maven repository in `/root/.m2` for Spark 3.5, as in the static images.
- JDK 17 at `/usr/lib/jvm/java-17-openjdk`, alongside the tag's default JDK, for the Spark unit tests.
- PySpark virtualenvs at `/opt/pyspark/3.4.4` and `/opt/pyspark/3.5.5`, one for each PySpark version
  the Spark unit tests use. Each has pandas 2.2.3, pyarrow 20.0.0, cython, regex and requests, on
  Python 3.9 (CentOS 8) or 3.11 (CentOS 9). To use one, put its `bin/` first in `PATH`, for example
  `echo /opt/pyspark/3.5.5/bin >> $GITHUB_PATH`. The system `python3` is not changed.
- `gh`, `jq`, procps-ng, which, findutils, gzip and tzdata. CentOS 8 also has patchelf. CentOS 9 also
  has `clang-tools-extra` (clang-tidy) and `regex` for `dev/check.py`.

### GPU image (`centos-9-jdk17-cuda13.1-cudf`)

| Tag | Base | Compiler | CUDA |
|---|---|---|---|
| `centos-9-jdk17-cuda13.1-cudf` | `quay.io/centos/centos:stream9` | gcc-toolset-14 | 13.1 (`CUDA_ARCHITECTURES=75`) |

It uses JDK 17 and has Gluten's native dependencies built with `--enable_gpu=ON`. See
[GPU support](../get-started/VeloxGPU.md).

### Runtime images (`*-runtime`)

The TPC jobs verify that the static native library runs on a plain OS. A build image can't be used
for that, because its Arrow and other libraries could mask a missing dependency. The runtime images
add only JDKs and CI tools to the OS image. All JDKs are installed side by side, and a job selects
one with `JAVA_HOME`.

| Tag | Base | JDKs (`/usr/lib/jvm/`) | Also contains |
|---|---|---|---|
| `centos-8-runtime` | `centos:8` (repos point at `vault.centos.org`) | `java-1.8.0-openjdk`, `java-11-openjdk`, `java-17-openjdk` | Celeborn 0.5.4 and 0.6.3 tarballs in `/opt` |
| `centos-9-runtime` | `quay.io/centos/centos:stream9` | `java-1.8.0-openjdk`, `java-11-openjdk`, `java-17-openjdk`, `java-21-openjdk` | |
| `ubuntu-22.04-runtime` | `ubuntu:22.04` | `java-{8,11,17,21,25}-openjdk-amd64` | Hadoop 3.3.6 in `/opt/hadoop-3.3.6`, MinIO and `mc` in `/usr/local/bin` |

All three have `gh`, `jq`, wget, procps and tzdata. `install-spark-deps.sh` uses the Hadoop and
MinIO already in the image instead of downloading them.

The runtime images contain no Velox, Gluten, Arrow or vcpkg, and jobs running in them compile no
C++. In a pull request, Velox and Gluten C++ are compiled once, by `build-native-lib-centos-8` in
`vcpkg-almalinux-8-gcc13`, which uploads `libvelox.so` and `libgluten.so` as an artifact. Each TPC
job then:
1. downloads that artifact into `cpp/build/releases/`;
2. builds the Gluten jar and `tools/gluten-it` with Maven, compiling Java and Scala only;
3. runs TPC-H and TPC-DS at scale factor 1 and compares the results with vanilla Spark
   (`gluten-it queries-compare`).

## How the images are published

`docker_image.yml` runs:
- **weekly**, on Sundays at 20:00 UTC;
- **on every push to `main`** that changes `dev/docker/`, `docker_image.yml`,
  `.github/workflows/util/install-spark-deps.sh` or
  `.github/workflows/util/install-spark-resources.sh`.

Every run rebuilds all images from scratch against the current `main` of `apache/gluten`. The
`vcpkg-*` and `centos-*-jdk*` Dockerfiles clone `GLUTEN_REPO` at `GLUTEN_BRANCH`, which default to
`https://github.com/apache/gluten` and `main`. The GPU Dockerfile always clones `main`.
Multi-arch tags are built on an x86 runner and an arm64 runner, pushed by digest, and combined into
a single tag by the `merge*` jobs.

An image is built for arm64 only if an arm64 CI job or the [dev containers](../../../developers/dev-container.md)
use it:
- `vcpkg-almalinux-8-gcc13`, `vcpkg-centos-9` and `centos-9-jdk8` are built for both amd64 and arm64;
- `centos-9-runtime` is built for arm64 only;
- all other images are built for amd64 only.

To use an amd64-only image on an arm64 host, pass `--platform linux/amd64` (it runs emulated).

Notes:
- Only `apache/*` can publish. Publishing needs the `DOCKERHUB_USER` and `DOCKERHUB_TOKEN` secrets,
  which forks and pull requests don't have. A pull request that changes a Dockerfile therefore
  publishes nothing. Its images show up after it is merged.
- A failed build keeps the previous image. If either architecture of a multi-arch tag fails, the
  `merge` job doesn't run and the tag stays at its old version. If a change doesn't seem to have
  landed, check the tag's last-updated date on Docker Hub.
- Tags are mutable. Each build overwrites its tags, so CI always gets the latest image. To
  reproduce an older environment, pull by digest (`apache/gluten@sha256:...`). The Iceberg harness
  pins its images this way (`pin_image` in `.github/workflows/util/iceberg-upstream/baseline.py`).
- Older tags are no longer rebuilt. Docker Hub still has release tags (`1.3-*`, `1.4-*`, `1.5-*`),
  `vcpkg-centos-7*`, `vcpkg-centos-8`, `vcpkg-centos-8-gcc13`, `centos-8`, `centos-8-dev`,
  `centos-8-jdk11`, `centos-9-jdk8-cudf`, `centos-9-jdk11`, `centos-9-jdk17-cuda12.9-cudf`,
  `ubuntu22-m2-cache` and `gluten-vcpkg-builder_*`, but none of them is updated any more.
  `docker_image.yml` only publishes images that a workflow on `main` uses. Tags that only release
  branches use stay on Docker Hub as they were last built:
  - `vcpkg-centos-7`: `branch-1.2` to `branch-1.6`
  - `vcpkg-centos-7-gcc13` and `vcpkg-centos-8-gcc13`: `branch-1.7`
  - `vcpkg-centos-8`: `branch-1.5` and `branch-1.6`
  - `centos-8-jdk11` and `centos-9-jdk8-cudf`: `branch-1.5` to `branch-1.7`

## Building an image locally

Run the build from the repository root. The Dockerfiles use `RUN --mount=type=bind` to mount helper
scripts such as `dev/docker/install-ci-tools.sh` from the build context, which requires BuildKit
(the default in current Docker).

```bash
docker buildx build --load \
  -f dev/docker/Dockerfile.centos9-dynamic-build \
  --build-arg JAVA_VERSION=17 \
  -t gluten-local:centos-9-jdk17 .
```

The helper scripts are taken from your working tree, but the dependencies in the image (Arrow, vcpkg
packages, `/root/.m2`) are built from the clone made inside the Dockerfile. To build the `vcpkg-*`
and `centos-*-jdk*` images from another branch, pass
`--build-arg GLUTEN_REPO=<repository URL> --build-arg GLUTEN_BRANCH=<branch>`.

The static and dynamic build images take from one to several hours to build, mostly because of Arrow
and the vcpkg ports.

---
layout: page
title: Velox Backend CI
nav_order: 6
parent: Developer Overview
---
# Velox Backend CI

GitHub Actions (GHA) workflows are defined under `.github/workflows/`.

## Docker Build
A weekly job defined in `docker_image.yml` builds the Docker images used for CI verification. The Dockerfiles (under `dev/docker/`) and their corresponding images are listed below:

file | images | comments
-- | -- | --
Dockerfile.centos7-gcc13-static-build | apache/gluten:vcpkg-centos-7-gcc13 | centos 7, static link, jdk8
Dockerfile.centos8-gcc13-static-build | apache/gluten:vcpkg-centos-8-gcc13 | centos 8, static link, jdk8
Dockerfile.centos8-dynamic-build | apache/gluten:centos-8-jdk8 | centos 8, dynamic link, jdk8
Dockerfile.centos8-dynamic-build | apache/gluten:centos-8-jdk11 | centos 8, dynamic link, jdk11
Dockerfile.centos8-dynamic-build | apache/gluten:centos-8-jdk17 | centos 8, dynamic link, jdk17
cudf/Dockerfile.centos-9-jdk17-cuda13.1-cudf | apache/gluten:centos-9-jdk17-cuda13.1-cudf | centos 9, dynamic link, jdk17

The Docker images can be found at [https://hub.docker.com/r/apache/gluten/tags](https://hub.docker.com/r/apache/gluten/tags).

## Vcpkg Caching
The Gluten main branch is pulled during the static build in Docker, and vcpkg caches binary data for all dependencies defined under `dev/vcpkg`.
This binary data is cached into `/var/cache/vcpkg`, and CI jobs can reuse it in later builds. Setting `VCPKG_BINARY_SOURCES=clear` in the
environment disables reuse of the vcpkg cache.

## Arrow Libs Pre-installation
Arrow libs are pre-installed in the Docker image, since they don't change often and don't need to be rebuilt on every run.

## .M2 Cache
Dependency libraries are pre-installed into `/root/.m2` via `mvn dependency:go-offline`. Spark is set to 3.5 by default.

## Ccache
Since the Docker image is rebuilt weekly, the ccache is mostly outdated, so it is removed from the image.

## Updating the Docker Image
The GitHub secrets `DOCKERHUB_USER` and `DOCKERHUB_TOKEN` are used to push Docker images to [Docker Hub](https://hub.docker.com/r/apache/gluten/tags).
Note that GitHub secrets are not accessible in PRs from forked repos.

## Delta Spark UT
`delta_spark_ut.yml` runs delta-io/delta's own `spark` test suite against a Gluten Velox bundle, so Gluten is
validated against a real Delta release.
A number of those tests fail today.
Not because Gluten declines to offload a plan -- that should fall back to vanilla Spark and the test should
still pass.
Some are real gaps (fallback not happening where it should, metrics that differ from vanilla, native-side
bugs), and some are expected: a test that asserts on the query plan sees a different plan once the scan or
operators are offloaded, which is by design rather than a defect.
So the job does not gate on "any failure": it compares each run against a committed baseline of known failures
in `.github/workflows/util/delta-spark-ut/known-failures.txt` and fails on a **new** failure, or on a baseline
test that starts **passing** (which means the baseline needs updating).
It also fails outright if a run produced no usable results -- missing or truncated JUnit reports, or fewer
shards than expected -- rather than passing on partial data.

It runs per PR only when Delta-relevant paths change (`gluten-delta/**`, `backends-velox/src-delta*/**`, or
the pipeline's own files), nightly at 05:00 UTC for full coverage, and on demand via `workflow_dispatch` --
use the manual run to check a Velox/core change against Delta before merging.

To refresh the baseline after fixing something, run the workflow with `update_baseline=true`, download the
`delta-spark-ut-known-failures` artifact, and use the `known-failures.txt` it contains to replace
`.github/workflows/util/delta-spark-ut/known-failures.txt` in the repo.
See [.github/workflows/util/delta-spark-ut/README.md](https://github.com/apache/gluten/blob/main/.github/workflows/util/delta-spark-ut/README.md)
for the gate, the flaky-test quarantine and baseline bootstrapping.
Open follow-ups are tracked in [#12743](https://github.com/apache/gluten/issues/12743).

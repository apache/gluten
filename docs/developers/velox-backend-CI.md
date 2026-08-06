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
cudf/Dockerfile | apache/gluten:centos-9-jdk8-cudf | centos 9, dynamic link, jdk8

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
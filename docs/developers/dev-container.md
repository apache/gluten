---
layout: page
title: Dev Container
nav_order: 18
parent: Developer Overview
---

# Develop Gluten in a Dev Container

Gluten ships a [Dev Container](https://containers.dev/) configuration at
[`.devcontainer/devcontainer.json`](https://github.com/apache/gluten/blob/main/.devcontainer/devcontainer.json)
so you can develop inside a pre-built Docker image with the Velox/Gluten native
toolchain already installed. This is the fastest way to get a working Gluten build
without manually installing JDK, Maven, GCC, vcpkg, and the rest of the native
dependencies on your host.

## What the default configuration does

The default configuration opens the workspace inside `apache/gluten:vcpkg-centos-9`
(static link, CentOS 9, GCC toolset 12) and runs
`./dev/ci-velox-buildstatic-centos-9.sh` as its `postCreateCommand` to perform the
initial native Velox + Gluten C++ build with vcpkg. After the post-create step
finishes, the container is ready for a Maven build (for example,
`mvn package -Pbackends-velox -DskipTests`).

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) (or any other OCI runtime supported by
  your editor) installed and running on the host, **or** a GitHub Codespaces-enabled
  account.
- Either:
  - [Visual Studio Code](https://code.visualstudio.com/) with the
    [Dev Containers extension](https://code.visualstudio.com/docs/devcontainers/containers),
    or
  - [GitHub Codespaces](https://docs.github.com/en/codespaces) (the
    `.devcontainer/devcontainer.json` is picked up automatically when you create a
    Codespace for the repository).

## Open the workspace in a Dev Container

In VS Code:

1. Open the cloned Gluten repository.
2. Run the **Dev Containers: Reopen in Container** command from the Command Palette
   (`F1`).
3. Wait for the image to be pulled and the `postCreateCommand` to finish. The first
   run includes the native build and can take a while depending on your machine.

In Codespaces, just create a new Codespace for the repository -- the same flow is
executed in the cloud.

## Available images

The default `apache/gluten:vcpkg-centos-9` is only one of several pre-built images
maintained by the Gluten community. Other images are available as well -- see the
full list at
[https://hub.docker.com/r/apache/gluten/tags](https://hub.docker.com/r/apache/gluten/tags).

The Dockerfiles that produce these images, and a summary of what each one is intended
for (CentOS 8/9, static vs. dynamic link, different JDK versions, CUDF, etc.), are
documented in [Velox Backend CI](./velox-backend-CI.md#docker-build).

## Customize the configuration

To use a different image -- for example a dynamically linked CentOS 8 image with
JDK 17 -- edit `.devcontainer/devcontainer.json` and update the `image` field, and
the `postCreateCommand` if the toolchain provided by the new image requires a
different build script:

```jsonc
{
    "name": "Gluten Velox Backend (centos-8-jdk17, dynamic link)",
    "image": "apache/gluten:centos-8-jdk17",
    "postCreateCommand": "./dev/builddeps-veloxbe.sh --run_setup_script=ON --enable_vcpkg=OFF --build_arrow=OFF"
}
```

You can also drop the `postCreateCommand` entirely if you prefer to run the native
build manually inside the container; see
[Build Gluten Velox backend in docker](./velox-backend-build-in-docker.md) for the
full set of build flags and the trade-offs between static and dynamic linking.

## Use the image without a Dev Container

The same images can be used directly with `docker run` if you do not want to use the
Dev Containers tooling:

```bash
docker run -it --rm -v "$PWD":/workspace -w /workspace apache/gluten:vcpkg-centos-9 bash
# then, inside the container:
./dev/ci-velox-buildstatic-centos-9.sh
```

See [Build Gluten Velox backend in docker](./velox-backend-build-in-docker.md) for
more involved build recipes (packaging the Gluten jar, dynamic-link builds, etc.).

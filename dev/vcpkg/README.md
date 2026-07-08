# Build Gluten + Velox in Vcpkg Environment

## Overview

Currently, the `builtin-baseline` set in `vcpkg.json` is the commit hash for the `2026.03.18` tag of vcpkg.
The versions of all dependency libraries are determined by their respective ports at this vcpkg version,
except for those overridden in `vcpkg.json`, `vcpkg-configuration.json`, and overlay ports.

## Build in docker

For main branch code, you can follow the commands below.
- Pull the docker image: `docker pull apache/gluten:vcpkg-centos-7-gcc13`
- Build native code: `bash dev/ci-velox-buildstatic-centos-7.sh`
- Build JVM code: `mvn clean install -Pbackends-velox -Pspark-3.5 -DskipTests`

The gluten packages will be placed in `$GLUTEN_REPO/package/target/gluten-velox-bundle-*.jar`.

## Setup build environment manually

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

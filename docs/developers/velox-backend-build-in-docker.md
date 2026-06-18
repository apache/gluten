---
layout: page
title: Build Gluten Velox backend in docker
nav_order: 7
parent: Developer Overview
---

Currently, we have two way to build Gluten, static link or dynamic link. 

# Static link
The static link approach builds all dependency libraries in vcpkg for both Velox and Gluten. It then statically links these libraries into libvelox.so and libgluten.so, enabling the build of Gluten on *any* Linux OS on x86 platforms with 64G memory. However we only verified on Centos-7/8/9 and Ubuntu 20.04/22.04. Please submit an issue if it fails on your OS.

Here is the dependency libraries required on target system, they are the essential libraries pre-installed in every Linux OS.
```
linux-vdso.so.1
librt.so.1
libpthread.so.0
libdl.so.2
libm.so.6
libc.so.6
/lib64/ld-linux-x86-64.so.2
```

The 'dockerfile' to build Gluten jar:

```
FROM apache/gluten:vcpkg-centos-7

# Build Gluten Jar
RUN source /opt/rh/devtoolset-11/enable && \
    git clone https://github.com/apache/gluten.git && \
    cd gluten && \
    ./dev/builddeps-veloxbe.sh --run_setup_script=OFF --enable_s3=ON --enable_gcs=ON --enable_abfs=ON --enable_vcpkg=ON --build_arrow=OFF && \
    mvn clean package -Pbackends-velox -Pceleborn -Piceberg -Pdelta -Pspark-3.4 -DskipTests
```
`enable_vcpkg=ON` enables the static link. Vcpkg packages are already pre-installed in the vcpkg-centos-7 image and can be reused automatically. The image is maintained by Gluten community.

The command builds Gluten jar in 'glutenimage':
```
docker build -t glutenimage -f dockerfile
```
The gluten jar can be copied from glutenimage:/gluten/package/target/gluten-velox-bundle-*.jar

# Dynamic link
The dynamic link approach needs to install the dependencies libraries. It then dynamically link the .so files into libvelox.so and libgluten.so. Currently, Centos-7/8/9 and
 Ubuntu 20.04/22.04 are supported to build Gluten Velox backend dynamically. 

The 'dockerfile' to build Gluten jar:

```
FROM apache/gluten:centos-8-jdk8

# Build Gluten Jar
RUN source /opt/rh/devtoolset-11/enable && \
    git clone https://github.com/apache/gluten.git && \
    cd gluten && \
    ./dev/builddeps-veloxbe.sh --run_setup_script=ON --enable_hdfs=ON --enable_vcpkg=OFF --build_arrow=OFF && \
    mvn clean package -Pbackends-velox -Pceleborn -Piceberg -Pdelta -Pspark-3.4 -DskipTests && \
    ./dev/build-thirdparty.sh
```
`enable_vcpkg=OFF` enables the dynamic link. Part of shared libraries are pre-installed in the image. You need to specify `--run_setup_script=ON` to install the rest of them. It then packages all dependency libraries into a jar by `build-thirdparty.sh`. 
Please note the image is built based on centos-8. It has risk to build and deploy the jar on other OSes.

The command builds Gluten jar in 'glutenimage':
```
docker build -t glutenimage -f dockerfile
```
The gluten jar can be copied from glutenimage:/gluten/package/target/gluten-velox-bundle-*.jar and glutenimage:/gluten/package/target/gluten-thirdparty-lib-*.jar

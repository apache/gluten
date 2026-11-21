---
layout: page
title: Dynamic OpenSSL with FIPS Support in vcpkg Build
nav_order: 8
parent: Getting-Started
---

# Dynamic OpenSSL with FIPS Support in vcpkg Build

## Overview

By default, Gluten's vcpkg build statically links OpenSSL into the native libraries (`libvelox.so` and `libgluten.so`). However, in environments requiring FIPS (Federal Information Processing Standards) compliance or dynamic OpenSSL linking, Gluten now supports building with dynamically linked OpenSSL.

This feature is particularly useful for:
- **FIPS compliance**: Organizations requiring FIPS 140-2/140-3 validated cryptographic modules
- **Security updates**: Easier OpenSSL security patching without rebuilding Gluten
- **System integration**: Using system-provided OpenSSL libraries

## Prerequisites

- vcpkg-based build system (requires `enable_vcpkg=ON`)
- OpenSSL development libraries installed on the system
- Supported platforms: x86_64 and aarch64 Linux

## Building with Dynamic OpenSSL

### Basic Build Command

To enable dynamic OpenSSL linking with FIPS support, set the `VCPKG_DYNAMIC_OPENSSL` environment variable:

```bash
export VCPKG_DYNAMIC_OPENSSL=ON
./dev/buildbundle-veloxbe.sh --enable_vcpkg=ON
```

## How It Works

When `VCPKG_DYNAMIC_OPENSSL=ON` is set:

1. **vcpkg Configuration**: The `dynamic-openssl` feature is enabled in `vcpkg.json`, which includes OpenSSL with FIPS support
2. **Triplet Override**: Custom vcpkg triplets (`x64-linux-avx.cmake` and `arm64-linux-neon.cmake`) detect the environment variable and switch OpenSSL from static to dynamic linkage
3. **Build Process**: The `init.sh` script adds the `--x-feature=dynamic-openssl` flag to vcpkg install
4. **Library Packaging**: OpenSSL shared libraries are excluded from static linking but must be available at runtime

> **_NOTE:_**
At runtime, `LD_LIBRARY_PATH` must include the OS-provided OpenSSL libraries, including `libssl.so`, `libcrypto.so`, and the FIPS-certified `fips.so`. These libraries must be available and loadable; otherwise, Gluten will fail to start.
At the time of the Gluten 1.7 release, Gluten is built and tested with OpenSSL `3.5.2`. Users should ensure that the OpenSSL libraries (`libssl.so` and `libcrypto.so`) available at runtime are compatible with those used during linking. To minimize the risk of ABI or API compatibility issues, we recommend using the same major OpenSSL version for both build-time and runtime environments.

## References

- [OpenSSL FIPS Module](https://www.openssl.org/docs/fips.html)
- [FIPS 140-2 Standard](https://csrc.nist.gov/publications/detail/fips/140/2/final)
- [vcpkg Documentation](https://vcpkg.io/)
- [OpenSSL Package in vcpkg](https://github.com/microsoft/vcpkg/blob/2025.09.17/ports/openssl/vcpkg.json)
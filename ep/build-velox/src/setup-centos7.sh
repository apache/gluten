#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -efx -o pipefail
# Some of the packages must be build with the same compiler flags
# so that some low level types are the same size. Also, disable warnings.
SCRIPTDIR=./scripts
source $SCRIPTDIR/setup-helper-functions.sh
DEPENDENCY_DIR=${DEPENDENCY_DIR:-/tmp/velox-deps}
CPU_TARGET="${CPU_TARGET:-avx}"
NPROC=$(getconf _NPROCESSORS_ONLN)
FMT_VERSION=10.1.1
BUILD_DUCKDB="${BUILD_DUCKDB:-true}"
BUILD_GEOS="${BUILD_GEOS:-true}"
export CFLAGS=$(get_cxx_flags $CPU_TARGET)  # Used by LZO.
export CXXFLAGS=$CFLAGS  # Used by boost.
export CPPFLAGS=$CFLAGS  # Used by LZO.
export PKG_CONFIG_PATH=/usr/local/lib64/pkgconfig:/usr/local/lib/pkgconfig:/usr/lib64/pkgconfig:/usr/lib/pkgconfig:$PKG_CONFIG_PATH
FB_OS_VERSION="v2024.07.01.00"
GEOS_VERSION="3.10.7"

# shellcheck disable=SC2037
SUDO="sudo -E"

function run_and_time {
  time "$@"
  { echo "+ Finished running $*"; } 2> /dev/null
}

function dnf_install {
  $SUDO dnf install -y -q --setopt=install_weak_deps=False "$@"
}

function yum_install {
  $SUDO yum install -y "$@"
}

function install_cmake {
  cd "${DEPENDENCY_DIR}"
  wget_and_untar https://cmake.org/files/v3.28/cmake-3.28.3.tar.gz cmake-3
  cd cmake-3
  ./bootstrap --prefix=/usr/local
  make -j$(nproc)
  $SUDO make install
  cmake --version
}

function install_ninja {
  cd "${DEPENDENCY_DIR}"
  github_checkout ninja-build/ninja v1.11.1 --depth 1
  cd ninja
  ./configure.py --bootstrap
  cmake -Bbuild-cmake
  cmake --build build-cmake
  $SUDO cp ninja /usr/local/bin/
}

function install_folly {
  cd "${DEPENDENCY_DIR}"
  wget_and_untar https://github.com/facebook/folly/archive/refs/tags/${FB_OS_VERSION}.tar.gz folly
  cmake_install folly -DBUILD_TESTS=OFF -DFOLLY_HAVE_INT128_T=ON -DFOLLY_NO_EXCEPTION_TRACER=ON
}

# not used
function install_conda {
  cd "${DEPENDENCY_DIR}"
  mkdir -p conda && pushd conda
  wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
  MINICONDA_PATH=/opt/miniconda-for-velox
  bash Miniconda3-latest-Linux-x86_64.sh -b -u $MINICONDA_PATH
  popd
}

function install_openssl {
  cd "${DEPENDENCY_DIR}"
  wget_and_untar https://github.com/openssl/openssl/archive/refs/tags/OpenSSL_1_1_1s.tar.gz openssl
  cd openssl
  ./config no-shared
  make depend
  make
  $SUDO make install
}

function install_gflags {
  cd "${DEPENDENCY_DIR}"
  wget_and_untar https://github.com/gflags/gflags/archive/v2.2.2.tar.gz gflags
  cmake_install gflags -DBUILD_SHARED_LIBS=ON -DBUILD_STATIC_LIBS=ON -DBUILD_gflags_LIB=ON -DLIB_SUFFIX=64 -DCMAKE_INSTALL_PREFIX:PATH=/usr/local
}

function install_glog {
  cd "${DEPENDENCY_DIR}"
  wget_and_untar https://github.com/google/glog/archive/v0.5.0.tar.gz glog
  cmake_install glog -DBUILD_SHARED_LIBS=ON -DBUILD_STATIC_LIBS=ON -DCMAKE_INSTALL_PREFIX:PATH=/usr/local
}

function install_snappy {
  cd "${DEPENDENCY_DIR}"
  wget_and_untar https://github.com/google/snappy/archive/1.1.8.tar.gz snappy
  cmake_install snappy -DSNAPPY_BUILD_TESTS=OFF
}

function install_dwarf {
  cd "${DEPENDENCY_DIR}"
  wget_and_untar https://github.com/davea42/libdwarf-code/archive/refs/tags/20210528.tar.gz dwarf
  pushd dwarf
  #local URL=https://github.com/davea42/libdwarf-code/releases/download/v0.5.0/libdwarf-0.5.0.tar.xz
  #local DIR=dwarf
  #mkdir -p "${DIR}"
  #wget -q --max-redirect 3 "${URL}"
  #tar -xf libdwarf-0.5.0.tar.xz -C "${DIR}"
  #cd dwarf/libdwarf-0.5.0
  ./configure --enable-shared=no
  make
  make check
  $SUDO make install
  popd
}

function install_re2 {
  cd "${DEPENDENCY_DIR}"
  wget_and_untar https://github.com/google/re2/archive/refs/tags/2023-03-01.tar.gz re2
  cd re2
  $SUDO make install
}

function install_flex {
  cd "${DEPENDENCY_DIR}"
  wget_and_untar https://github.com/westes/flex/releases/download/v2.6.4/flex-2.6.4.tar.gz flex
  cd flex
  ./autogen.sh
  ./configure
  $SUDO make install
}

function install_lzo {
  cd "${DEPENDENCY_DIR}"
  wget_and_untar http://www.oberhumer.com/opensource/lzo/download/lzo-2.10.tar.gz lzo
  cd lzo
  ./configure --prefix=/usr/local --enable-shared --disable-static --docdir=/usr/local/share/doc/lzo-2.10
  make "-j$(nproc)"
  $SUDO make install
}

function install_boost {
  # Remove old version.
  sudo rm -f /usr/local/lib/libboost_* /usr/lib64/libboost_* /opt/rh/devtoolset-11/root/usr/lib64/dyninst/libboost_*
  sudo rm -rf "${DEPENDENCY_DIR}"/boost/ /usr/local/include/boost/ /usr/local/lib/cmake/Boost-1.72.0/
  cd "${DEPENDENCY_DIR}"
  wget_and_untar https://github.com/boostorg/boost/releases/download/boost-1.84.0/boost-1.84.0.tar.gz boost
  cd boost
  ./bootstrap.sh --prefix=/usr/local --with-python=/usr/bin/python3 --with-python-root=/usr/lib/python3.6 --without-libraries=python
  $SUDO ./b2 "-j$(nproc)" -d0 install threading=multi
}

function install_protobuf {
  cd "${DEPENDENCY_DIR}"
  wget_and_untar https://github.com/protocolbuffers/protobuf/releases/download/v21.4/protobuf-all-21.4.tar.gz protobuf
  pushd protobuf
  ./configure  CXXFLAGS="-fPIC"  --prefix=/usr/local
  make "-j$(nproc)"
  $SUDO make install
  popd
}

function install_gtest {
  cd "${DEPENDENCY_DIR}"
  wget_and_untar https://github.com/google/googletest/archive/refs/tags/release-1.12.1.tar.gz googletest
  pushd googletest
  mkdir -p build && cd build && cmake -DBUILD_GTEST=ON -DBUILD_GMOCK=ON -DINSTALL_GTEST=ON -DINSTALL_GMOCK=ON -DBUILD_SHARED_LIBS=ON ..
  make "-j$(nproc)"
  $SUDO make install
  popd
}

function install_fmt {
  wget_and_untar https://github.com/fmtlib/fmt/archive/10.1.1.tar.gz fmt
  rm -rf /usr/local/lib64/libfmt.a
  rm -rf /usr/local/lib64/cmake/fmt
  rm -rf  /usr/local/include/fmt
  cmake_install fmt -DFMT_TEST=OFF
}

function install_duckdb {
  if $BUILD_DUCKDB ; then
    echo 'Building DuckDB'
    wget_and_untar https://github.com/duckdb/duckdb/archive/refs/tags/v0.8.1.tar.gz duckdb
    cmake_install duckdb -DBUILD_UNITTESTS=OFF -DENABLE_SANITIZER=OFF -DENABLE_UBSAN=OFF -DBUILD_SHELL=OFF -DEXPORT_DLL_SYMBOLS=OFF -DCMAKE_BUILD_TYPE=Release
  fi
}

function install_geos {
  if [[ "$BUILD_GEOS" == "true" ]]; then
    wget_and_untar https://github.com/libgeos/geos/archive/${GEOS_VERSION}.tar.gz geos
    cmake_install_dir geos -DBUILD_TESTING=OFF
  fi
}

function install_prerequisites {
  run_and_time install_lzo
  run_and_time install_boost
  run_and_time install_re2
  run_and_time install_flex
  run_and_time install_openssl
  run_and_time install_gflags
  run_and_time install_glog
  run_and_time install_snappy
  run_and_time install_dwarf
}

function install_velox_deps {
  run_and_time install_fmt
  run_and_time install_folly
  run_and_time install_protobuf
  run_and_time install_gtest
  run_and_time install_duckdb
  run_and_time install_geos
}

$SUDO dnf makecache

# dnf install dependency libraries
dnf_install epel-release dnf-plugins-core # For ccache, ninja
dnf_install ccache wget which libevent-devel \
  yasm \
  openssl-devel libzstd-devel lz4-devel double-conversion-devel \
  curl-devel libxml2-devel libgsasl-devel libuuid-devel patch libicu-devel tzdata

$SUDO dnf remove -y gflags

# Required for Thrift
dnf_install autoconf automake libtool bison python3 python3-devel

# Required for build flex
dnf_install gettext-devel texinfo help2man

$SUDO yum makecache
yum_install centos-release-scl
yum_install devtoolset-11
source /opt/rh/devtoolset-11/enable || exit 1
gcc --version
set -u

# Build from source
[ -d "$DEPENDENCY_DIR" ] || mkdir -p "$DEPENDENCY_DIR"

run_and_time install_cmake
run_and_time install_ninja

install_prerequisites
install_velox_deps

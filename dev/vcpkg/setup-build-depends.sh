#! /bin/bash

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

set -e

## Install functions begin

function semver {
    echo "$@" | awk -F. '{ printf("%d%03d%03d", $1,$2,$3); }'
}

install_maven_from_source() {
    if [ -z "$(which mvn)" ]; then
        maven_version=3.9.12
        maven_install_dir=/opt/maven-$maven_version
        if [ -d /opt/maven-$maven_version ]; then
            echo "Failed to install maven: ${maven_install_dir} is exists" >&2
            exit 1
        fi

        cd /tmp
        local_binary="apache-maven-${maven_version}-bin.tar.gz";
        mirror_host="https://www.apache.org/dyn/closer.lua";
        url="${mirror_host}/maven/maven-3/${maven_version}/binaries/${local_binary}?action=download";
        wget -nv -O ${local_binary} ${url};
        tar -xvf ${local_binary};
        rm -rf ${local_binary};
        mv apache-maven-$maven_version "${maven_install_dir}"
        ln -s "${maven_install_dir}/bin/mvn" /usr/local/bin/mvn
    fi
}

install_gcc11_from_source() {
     cur_gcc_version=$(gcc -dumpversion)
     if [ "$(semver "$cur_gcc_version")" -lt "$(semver 11.0.0)" ]; then
            gcc_version=gcc-11.5.0
            gcc_install_dir=/usr/local/${gcc_version}
            cd /tmp
            if [ ! -d $gcc_version ]; then
                GCC_URL1=https://ftp.gnu.org/gnu/gcc/${gcc_version}/${gcc_version}.tar.gz
                GCC_URL2=https://www.mirrorservice.org/sites/ftp.gnu.org/gnu/gcc/${gcc_version}/${gcc_version}.tar.gz
                wget ${GCC_URL1} || wget ${GCC_URL2}
                tar -xvf ${gcc_version}.tar.gz
            fi
            cd ${gcc_version}
            ./contrib/download_prerequisites

            mkdir gcc-build && cd gcc-build
            ../configure --prefix=${gcc_install_dir} --disable-multilib --enable-languages=c,c++
            make -j$(nproc)
            make install

            update-alternatives --install /usr/bin/gcc gcc /usr/local/${gcc_version}/bin/gcc 900 --slave /usr/bin/g++ g++ /usr/local/${gcc_version}/bin/g++
     fi
}

install_centos_7() {
    export PATH=/usr/local/bin:$PATH

    sed -i \
        -e 's/^mirrorlist/#mirrorlist/' \
        -e 's/^# *baseurl *=/baseurl=/' \
        -e 's/mirror\.centos\.org/vault.centos.org/' \
        /etc/yum.repos.d/*.repo

    yum -y install epel-release centos-release-scl
    sed -i \
        -e 's/^mirrorlist/#mirrorlist/' \
        -e 's/^# *baseurl *=/baseurl=/' \
        -e 's/mirror\.centos\.org/vault.centos.org/' \
        /etc/yum.repos.d/*.repo

    yum -y install \
        wget curl tar zip unzip which patch sudo \
        ninja-build perl-IPC-Cmd autoconf autoconf-archive automake libtool \
        devtoolset-11 python3 pip dnf \
        bison \
        java-1.8.0-openjdk java-1.8.0-openjdk-devel

    pip3 install --upgrade pip

    # Requires cmake >= 3.28.3
    pip3 install cmake==3.28.3

    # Requires git >= 2.7.4
    if [[ "$(git --version)" != "git version 2."* ]]; then
        yum -y remove git
        # Requires 'centos-release-scl' package to be installed.
        yum -y install rh-git227
        source /opt/rh/rh-git227/enable
        echo "source /opt/rh/rh-git227/enable" >> ~/.bashrc
    fi

    # flex>=2.6.0
    if [[ "$(PATH="/usr/local/bin:$PATH" flex --version 2>&1)" != "flex 2.6."* ]]; then
        yum -y install gettext-devel
        FLEX_URL="https://github.com/westes/flex/releases/download/v2.6.4/flex-2.6.4.tar.gz"
        mkdir -p /tmp/flex
        wget -q --max-redirect 3 -O - "${FLEX_URL}" | tar -xz -C /tmp/flex --strip-components=1
        cd /tmp/flex
        ./autogen.sh
        ./configure
        make install
        cd
        rm -rf /tmp/flex
    fi

    # automake>=1.14
    installed_automake_version="$(aclocal --version | sed -En "1s/^.* ([1-9\.]*)$/\1/p")"
    if [ "$(semver "$installed_automake_version")" -lt "$(semver 1.14)" ]; then
        mkdir -p /tmp/automake
        AUTOMAKE_URL1="https://ftp.gnu.org/gnu/automake/automake-1.16.5.tar.xz"
        AUTOMAKE_URL2="https://www.mirrorservice.org/sites/ftp.gnu.org/gnu/automake/automake-1.16.5.tar.xz"
        wget -O /tmp/automake.tar.xz "$AUTOMAKE_URL1" || wget -O /tmp/automake.tar.xz "$AUTOMAKE_URL2"
        tar -xf /tmp/automake.tar.xz -C /tmp/automake --strip-components=1
        rm -f /tmp/automake.tar.xz
        cd /tmp/automake
        ./configure
        make install -j
        cd
        rm -rf /tmp/automake

        # Fix aclocal search path
        echo /usr/share/aclocal > /usr/local/share/aclocal/dirlist
    fi

    install_maven_from_source
}

install_centos_8() {
    sed -i \
        -e 's/^mirrorlist/#mirrorlist/' \
        -e 's/^# *baseurl *=/baseurl=/' \
        -e 's/mirror\.centos\.org/vault.centos.org/' \
        /etc/yum.repos.d/*.repo

    yum -y install \
        wget curl tar zip unzip git which sudo patch \
        cmake perl-IPC-Cmd autoconf automake libtool \
        gcc-toolset-11 \
        flex bison python3 \
        java-1.8.0-openjdk java-1.8.0-openjdk-devel

    pip3 install --upgrade pip

    # Requires cmake >= 3.28.3
    pip3 install cmake==3.28.3

    dnf -y --enablerepo=powertools install autoconf-archive ninja-build

    install_maven_from_source
}

install_centos_9() {
    yum -y install \
        wget tar zip unzip git which sudo patch \
        cmake perl-IPC-Cmd autoconf automake libtool \
        gcc-toolset-12 \
        flex bison python3 python3-pip \
        java-17-openjdk java-17-openjdk-devel

    pip3 install --upgrade pip

    # Requires cmake >= 3.28.3
    pip3 install cmake==3.28.3

    dnf -y --enablerepo=crb install autoconf-archive ninja-build

    install_maven_from_source
}

install_ubuntu_20.04() {
    apt-get update && apt-get -y install \
        wget curl tar zip unzip git \
        build-essential ccache cmake ninja-build pkg-config autoconf autoconf-archive libtool \
        flex bison \
        openjdk-8-jdk maven
    # Overwrite gcc-9 installed by build-essential.
    sudo apt install -y software-properties-common
    sudo add-apt-repository ppa:ubuntu-toolchain-r/test
    sudo apt update && sudo apt install -y gcc-11 g++-11
    sudo ln -sf /usr/bin/gcc-11 /usr/bin/gcc
    sudo ln -sf /usr/bin/g++-11 /usr/bin/g++
}

install_ubuntu_22.04() { install_ubuntu_20.04; }

install_openeuler_24.03() {
    dnf -y update
    dnf -y install dnf-plugins-core
    dnf -y update
    dnf -y install wget curl tar zip unzip git which bison flex \
        gcc g++ cmake ninja-build perl-IPC-Cmd autoconf autoconf-archive automake libtool \
        java-1.8.0-openjdk java-1.8.0-openjdk-devel python3-devel python3-pip libstdc++-static

    pip install cmake==3.28.3

    install_maven_from_source
}

install_alinux_3() {
    yum -y groupinstall "Development Tools"
    yum -y install \
        wget curl tar zip unzip git which \
        cmake ninja-build perl-IPC-Cmd autoconf autoconf-archive automake libtool \
        libstdc++-static flex bison python3 \
        java-1.8.0-openjdk java-1.8.0-openjdk-devel
}

install_tencentos_3.2() {
    yum -y install \
        wget curl tar zip unzip git which \
        cmake ninja-build perl-IPC-Cmd autoconf autoconf-archive automake libtool \
        gcc-toolset-11 \
        flex bison python3 \
        java-8-konajdk

    install_maven_from_source
}

install_debian_10() {
    apt-get -y install \
        wget curl tar zip unzip git apt-transport-https \
        build-essential ccache cmake ninja-build pkg-config autoconf autoconf-archive libtool \
        flex bison python3

    # Download the Eclipse Adoptium GPG key
    wget -qO - https://packages.adoptium.net/artifactory/api/gpg/key/public | gpg --dearmor | tee /etc/apt/trusted.gpg.d/adoptium.gpg > /dev/null

    # Configure the Eclipse Adoptium repository
    echo "deb https://packages.adoptium.net/artifactory/deb $(awk -F= '/^VERSION_CODENAME/{print$2}' /etc/os-release) main" | tee /etc/apt/sources.list.d/adoptium.list

    # Install JDK
    apt update && apt-get -y install temurin-8-jdk

    install_maven_from_source
    install_gcc11_from_source
}

install_debian_11() {
    apt-get -y install \
        wget curl tar zip unzip git apt-transport-https \
        build-essential ccache cmake ninja-build pkg-config autoconf autoconf-archive libtool \
        flex bison

    # Download the Eclipse Adoptium GPG key
    wget -qO - https://packages.adoptium.net/artifactory/api/gpg/key/public | gpg --dearmor | tee /etc/apt/trusted.gpg.d/adoptium.gpg > /dev/null

    # Configure the Eclipse Adoptium repository
    echo "deb https://packages.adoptium.net/artifactory/deb $(awk -F= '/^VERSION_CODENAME/{print$2}' /etc/os-release) main" | tee /etc/apt/sources.list.d/adoptium.list

    # Install JDK
    apt update && apt-get -y install temurin-8-jdk
}

## Install function end

## Usage
if [ -n "$*" ]; then
    cat <<EOF
Usage: sudo $0
Install build dependencies on the system.
EOF
    exit 1
fi

## Check and install depends

log_fatal() {
    echo "$@" >&2
    exit 1
}

[ "$(id -u)" == 0 ] || log_fatal "Root is required"

[ -f /etc/os-release ] || log_fatal "Failed to detect os: /etc/os-release in not exists"
eval "$(sed -En "/^(VERSION_|)ID=/s/^/OS_/p" /etc/os-release)"

[ -n "$OS_ID" -a -n "$OS_VERSION_ID" ] || log_fatal "Failed to detect os: ID or VERSION_ID is empty"

INSTALL_FUNC="install_$(echo "$OS_ID" | tr 'A-Z' 'a-z')_${OS_VERSION_ID}"
[ "$(type -t "$INSTALL_FUNC")" == function ] || log_fatal "Unsupport OS: ${OS_ID} ${OS_VERSION_ID}"

set -x
$INSTALL_FUNC

echo "Success" >&2

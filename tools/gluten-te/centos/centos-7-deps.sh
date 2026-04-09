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

set -ex

sed -i -e "s/enabled=1/enabled=0/" /etc/yum/pluginconf.d/fastestmirror.conf
sed -i -e "s|mirrorlist=|#mirrorlist=|g" /etc/yum.repos.d/CentOS-*
sed -i -e "s|#baseurl=http://mirror.centos.org|baseurl=http://vault.centos.org|g" /etc/yum.repos.d/CentOS-*
yum install -y centos-release-scl
rm -f /etc/yum.repos.d/CentOS-SCLo-scl.repo
sed -i \
  -e 's/^mirrorlist/#mirrorlist/' \
  -e 's/^#baseurl/baseurl/' \
  -e 's/mirror\.centos\.org/vault.centos.org/' \
  /etc/yum.repos.d/CentOS-SCLo-scl-rh.repo

yum -y install epel-release
yum -y install \
  wget curl tar zip unzip which patch sudo \
  ninja-build perl-IPC-Cmd autoconf autoconf-archive automake libtool \
  devtoolset-11 python3 pip dnf \
  bison \
  java-1.8.0-openjdk java-1.8.0-openjdk-devel

# Link c++ to the one in devtoolset.
ln -s /opt/rh/devtoolset-11/root/usr/bin/c++ /usr/bin/c++

semver() {
    echo "$@" | awk -F. '{ printf("%d%05d%05d", $1,$2,$3); }'
}

pip3 install --upgrade pip

# cmake >= 3.28.3
pip3 install cmake==3.28.3

# git >= 2.7.4
if [[ "$(git --version)" != "git version 2."* ]]; then
  [ -f /etc/yum.repos.d/ius.repo ] || yum -y install https://repo.ius.io/ius-release-el7.rpm
  yum -y remove git
  yum -y install git236
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
  echo /usr/share/aclocal >/usr/local/share/aclocal/dirlist
fi

# cmake
if [ -z "$(which mvn)" ]; then
  maven_version=3.9.2
  maven_install_dir=/opt/maven-$maven_version
  if [ -d /opt/maven-$maven_version ]; then
    echo "Failed to install maven: ${maven_install_dir} is exists" >&2
    exit 1
  fi

  cd /tmp
  wget https://archive.apache.org/dist/maven/maven-3/$maven_version/binaries/apache-maven-$maven_version-bin.tar.gz
  tar -xvf apache-maven-$maven_version-bin.tar.gz
  rm -f apache-maven-$maven_version-bin.tar.gz
  mv apache-maven-$maven_version "${maven_install_dir}"
  ln -s "${maven_install_dir}/bin/mvn" /usr/local/bin/mvn
fi

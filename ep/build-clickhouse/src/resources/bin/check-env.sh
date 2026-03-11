#!/bin/bash
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

export GLUTEN_HOME=$(cd -P -- "$(dirname -- "$0")/.." && pwd -P)

function check_os_version() {
    if [[ "$OSTYPE" == "linux-gnu"* ]]; then
      OS_DISTRIBUTION=$(cat /etc/os-release | grep "^ID=" | cut -d "=" -f 2 | tr -d '"')
      OS_VERSION_ID=$(cat /etc/os-release | grep "^VERSION_ID=" | cut -d "=" -f 2 | tr -d '"')
    elif [[ "$OSTYPE" == "darwin"* ]]; then
      OS_DISTRIBUTION="macos"
      OS_VERSION_ID=$(sw_vers -productVersion)
    else
      echo "Unsupported OS: $OSTYPE"
      exit 1
    fi
    OS_VERSION=${OS_DISTRIBUTION}${OS_VERSION_ID}
    OS_ARCH=$(uname -m)

    # compare with BUILD_INFO in parent folder
    BUILD_INFO_FILE=$(cd $(dirname -- $0)/..; pwd -P)/BUILD_INFO
    if [[ ! -f "${BUILD_INFO_FILE}" ]]; then
        echo "[ERROR] file BUILD_INFO not found."
        exit 1
    fi
    BUILD_OS_VERSION=$(cat ${BUILD_INFO_FILE} | grep "^OS_VERSION=" | cut -d "=" -f 2)
    BUILD_OS_ARCH=$(cat ${BUILD_INFO_FILE} | grep "^OS_ARCH=" | cut -d "=" -f 2)
    if [[ "${OS_VERSION}" != "${BUILD_OS_VERSION}" ]]; then
        echo "[WARNING] OS_VERSION ${OS_VERSION} is not same as ${BUILD_OS_VERSION} which is defined in BUILD_INFO."
    fi
    if [[ "${OS_ARCH}" != "${BUILD_OS_ARCH}" ]]; then
        echo "[ERROR] OS_ARCH ${OS_ARCH} is not supported, please download ${BUILD_OS_ARCH} package"
        exit 1
    fi
}

check_os_version

function check_java_version() {
    version=$(java -version 2>&1 | awk -F\" '/version/ {print $2}')
    version_first_part="$(echo ${version} | cut -d '.' -f1)"
    version_second_part="$(echo ${version} | cut -d '.' -f2)"
    if [[ "$version_first_part" -eq "1" ]] && [[ "$version_second_part" -eq "8" ]]; then
        # jdk version: 1.8.0-332
        echo "JAVA_VERSION=${version}"
    elif [[ "$version_first_part" -ge "8" ]]; then
        # jdk version: 11.0.15 / 17.0.3
        echo "[WARNING] jdk version ${version} is not verified"
    else
        echo "[ERROR] jdk version ${version} is not supported, please use jdk 1.8+"
        exit 1
    fi
}

check_java_version

function check_spark_version() {
  if [ -z "${SPARK_HOME}" ]; then
    if [ -d "${GLUTEN_HOME}/spark" ]; then
      export SPARK_HOME=${GLUTEN_HOME}/spark
    else
      echo "SPARK_HOME is not set" >&2
      exit 1
    fi
  fi
  echo "SPARK_HOME=${SPARK_HOME}"
  SPARK_VERSION=$(cat ${SPARK_HOME}/RELEASE | grep "^Spark" | cut -d " " -f 2)
  SPARK_MAJOR_MINOR_VERSION=$(echo ${SPARK_VERSION} | cut -d '.' -f 1-2)
  if [[ "${SPARK_MAJOR_MINOR_VERSION}" != "3.3" ]] && [[ "${SPARK_MAJOR_MINOR_VERSION}" != "3.4" ]] && [[ "${SPARK_MAJOR_MINOR_VERSION}" != "3.5" ]] && [[ "${SPARK_MAJOR_MINOR_VERSION}" != "4.0" ]]; then
    echo "[ERROR] SPARK_VERSION ${SPARK_VERSION} which defined in $SPARK_HOME/RELEASE, is not supported. Please use spark 3.3, 3.4, 3.5, or 4.0."
    exit 1
  fi
  export SPARK_MAJOR_MINOR_VERSION=${SPARK_MAJOR_MINOR_VERSION}
  echo "SPARK_VERSION=${SPARK_VERSION}"
}

check_spark_version
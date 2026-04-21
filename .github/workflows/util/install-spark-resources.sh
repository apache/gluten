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

# Download Spark resources, required by some Spark UTs. The resource path should be set
# for spark.test.home in mvn test.
#
# This file can be:
# Executed directly: ./install-spark-resources.sh <spark-version> [install-dir]

set -e

# Installs Spark binary and source releases with:
# 1 - spark version
# 2 - hadoop version
# 3 - scala version
function install_spark() {
  local spark_version="$1"
  local hadoop_version="$2"
  local scala_version="$3"
  local spark_version_short=$(echo "${spark_version}" | cut -d '.' -f 1,2 | tr -d '.')
  local scala_suffix=$([ "${scala_version}" == '2.13' ] && echo '-scala-2.13' || echo '')
  local scala_suffix_short=$([ "${scala_version}" == '2.13' ] && echo '-scala2.13' || echo '')
  local mirror_host='https://www.apache.org/dyn/closer.lua/'
  local mirror_host2='https://mirror.lyrahosting.com/apache/' # Fallback mirror due to closer.lua slowness
  local url_query='?action=download'
  local checksum_suffix='sha512'
  local url_path="spark/spark-${spark_version}/"
  local local_binary="spark-${spark_version}-bin-hadoop${hadoop_version}${scala_suffix_short}.tgz"
  local local_binary_checksum="${local_binary}.${checksum_suffix}"
  local local_source="spark-${spark_version}.tgz"
  local local_source_checksum="${local_source}.${checksum_suffix}"
  local remote_binary="${mirror_host2}${url_path}${local_binary}${url_query}"
  local remote_binary_checksum="${mirror_host}${url_path}${local_binary_checksum}${url_query}"
  local remote_source="${mirror_host2}${url_path}${local_source}${url_query}"
  local remote_source_checksum="${mirror_host}${url_path}${local_source_checksum}${url_query}"
  local wget_opts="--no-verbose --no-check-certificate"

  wget ${wget_opts} -O "${local_binary}" "${remote_binary}"
  wget ${wget_opts} -O "${local_source}" "${remote_source}"

  # Checksum may not have been specified; don't check if doesn't exist
  if [ "$(command -v shasum)" ]; then
    wget ${wget_opts} -O "${local_binary_checksum}" "${remote_binary_checksum}"
    if ! shasum -a 512 -c "${local_binary_checksum}" > /dev/null ; then
      echo "Bad checksum from ${remote_binary_checksum}"
      rm -f "${local_binary_checksum}"
      exit 2
    fi
    rm -f "${local_binary_checksum}"

    wget ${wget_opts} -O "${local_source_checksum}" "${remote_source_checksum}"
    if ! shasum -a 512 -c "${local_source_checksum}" > /dev/null ; then
      echo "Bad checksum from ${remote_source_checksum}"
      rm -f "${local_source_checksum}"
      exit 2
    fi
    rm -f "${local_source_checksum}"
  else
    echo "Skipping checksum because shasum is not installed." 1>&2
  fi

  tar --strip-components=1 -xf "${local_binary}" spark-"${spark_version}"-bin-hadoop"${hadoop_version}""${scala_suffix_short}"/jars/ \
    spark-"${spark_version}"-bin-hadoop"${hadoop_version}""${scala_suffix_short}"/python \
    spark-"${spark_version}"-bin-hadoop"${hadoop_version}""${scala_suffix_short}"/bin
  mkdir -p ${INSTALL_DIR}/shims/spark"${spark_version_short}""${scala_suffix}"/spark_home/assembly/target/scala-"${scala_version}"
  mv jars ${INSTALL_DIR}/shims/spark"${spark_version_short}""${scala_suffix}"/spark_home/assembly/target/scala-"${scala_version}"
  mv python ${INSTALL_DIR}/shims/spark"${spark_version_short}""${scala_suffix}"/spark_home
  mv bin ${INSTALL_DIR}/shims/spark"${spark_version_short}""${scala_suffix}"/spark_home

  tar --strip-components=1 -xf "${local_source}" spark-"${spark_version}"/sql/core/src/test/resources/
  mkdir -p shims/spark"${spark_version_short}${scala_suffix}"/spark_home/
  mv sql shims/spark"${spark_version_short}${scala_suffix}"/spark_home/

  rm -rf "${local_binary}"
  rm -rf "${local_source}"
}

# Only run install_spark when script is executed directly (not sourced)
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  INSTALL_DIR=${2:-/opt/}
  mkdir -p ${INSTALL_DIR}

  case "$1" in
  3.3)
      # Spark-3.3
      cd ${INSTALL_DIR} && \
      install_spark "3.3.1" "3" "2.12"
      ;;
  3.4)
      # Spark-3.4
      cd ${INSTALL_DIR} && \
      install_spark "3.4.4" "3" "2.12"
      ;;
  3.5)
      # Spark-3.5
      cd ${INSTALL_DIR} && \
      install_spark "3.5.5" "3" "2.12"
      ;;
  3.5-scala2.13)
      # Spark-3.5, scala 2.13
      cd ${INSTALL_DIR} && \
      install_spark "3.5.5" "3" "2.13"
      ;;
  4.0)
      # Spark-4.0, scala 2.12 // using 2.12 as a hack as 4.0 does not have 2.13 suffix
      cd ${INSTALL_DIR} && \
      install_spark "4.0.1" "3" "2.12"
      mv /opt/shims/spark40/spark_home/assembly/target/scala-2.12 /opt/shims/spark40/spark_home/assembly/target/scala-2.13
      ;;
  4.1)
      # Spark-4.x, scala 2.12 // using 2.12 as a hack as 4.0 does not have 2.13 suffix
      cd ${INSTALL_DIR} && \
      install_spark "4.1.1" "3" "2.12"
      mv /opt/shims/spark41/spark_home/assembly/target/scala-2.12 /opt/shims/spark41/spark_home/assembly/target/scala-2.13
      ;;
  *)
      echo "Spark version is expected to be specified."
      exit 1
      ;;
  esac
fi

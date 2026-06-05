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

set -e

export GLUTEN_HOME=$(cd -P -- "$(dirname -- "$0")/.." && pwd -P)
source ${GLUTEN_HOME}/bin/check-env.sh || exit 1

[[ ! -d "${GLUTEN_HOME}"/logs ]] && mkdir -p "${GLUTEN_HOME}"/logs

function start() {
  if [[ $(ps -ef | grep -v grep | grep -c "org.apache.spark.sql.hive.thriftserver.HiveThriftServer2") -gt 0 ]]; then
    echo "Gluten spark thriftserver is already running. Please stop it first."
    exit 1
  fi

  # filter out empty lines and comments and spark.driver.extraJavaOptions
  GLUTEN_OPTIONS=$(cat "${GLUTEN_HOME}"/conf/gluten.properties | grep -v "^#" | grep -v "^$" | grep -v "spark.driver.extraJavaOptions" | awk '{print "--conf "$0}' | tr '\n' ' ')
  [[ -n "${HADOOP_CONF_DIR}" ]] && GLUTEN_OPTIONS=${GLUTEN_OPTIONS} \
    " --conf spark.gluten.sql.columnar.backend.ch.runtime_config.hdfs.libhdfs3_conf=${HADOOP_CONF_DIR}/hdfs-site.xml"

  DRIVER_OPTIONS=${DRIVER_OPTIONS:-"-Dlog4j.configuration=file:${GLUTEN_HOME}/conf/log4j.properties"}
  DRIVER_OPTIONS="${DRIVER_OPTIONS} $(cat ${GLUTEN_HOME}/conf/gluten.properties | grep "^spark.driver.extraJavaOptions" | cut -d "=" -f 2)"

  GLUTEN_JARS=
  if [ "${SPARK_MAJOR_MINOR_VERSION}" == "3.2" ]; then
      GLUTEN_JARS=${GLUTEN_HOME}/jars/spark32/*
  elif [ "${SPARK_MAJOR_MINOR_VERSION}" == "3.3" ]; then
      GLUTEN_JARS=${GLUTEN_HOME}/jars/spark33/*
  else
      echo "Unsupported spark version: ${SPARK_MAJOR_MINOR_VERSION}"
      exit 1
  fi
  echo "GLUTEN_JARS: ${GLUTEN_JARS} will be loaded."

  export LD_PRELOAD=${GLUTEN_HOME}/libs/libch.so
  export SPARK_LOG_DIR=${GLUTEN_HOME}/logs

  rm -f ${GLUTEN_HOME}/logs/spark-*.out*
  nohup ${SPARK_HOME}/sbin/start-thriftserver.sh \
    --properties-file ${GLUTEN_HOME}/conf/spark-default.conf \
    --conf spark.driver.extraClassPath=${GLUTEN_JARS} \
    --conf spark.executor.extraClassPath=${GLUTEN_JARS} \
    --conf spark.driver.extraJavaOptions=${DRIVER_OPTIONS} \
    --conf spark.gluten.sql.columnar.libpath=${GLUTEN_HOME}/libs/libch.so \
    --verbose \
    ${GLUTEN_OPTIONS} \
    > /dev/null 2>&1 &

  echo "Gluten spark thriftserver is started. Please check logs in ${GLUTEN_HOME}/logs"
}

function stop() {
  ${SPARK_HOME}/sbin/stop-thriftserver.sh
  sleep 5
  if [[ $(ps -ef | grep -v grep | grep -c "org.apache.spark.sql.hive.thriftserver.HiveThriftServer2") -eq 0 ]]; then
    echo "Gluten spark thriftserver is stopped."
  else
    echo "Gluten spark thriftserver is still running. Please stop it manually."
    exit 1
  fi
}

[[ $# -eq 0 ]] && echo "No arguments provided. Usage: $0 [start|stop|restart]" && exit 1
[[ "$1" == "start" ]] && start && exit 0
[[ "$1" == "stop" ]] && stop && exit 0
[[ "$1" == "restart" ]] && stop && start && exit 0

echo "Invalid argument: $1. Usage: $0 [start|stop|restart]" && exit 1
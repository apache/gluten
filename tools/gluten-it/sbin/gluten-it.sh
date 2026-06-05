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

set -euf

BASEDIR=$(dirname $0)

LIB_DIR=$BASEDIR/../package/target/lib
if [[ ! -d $LIB_DIR ]]; then
  echo "Lib directory not found at $LIB_DIR. Please build gluten-it first. For example: mvn clean install"
  exit 1
fi

JAR_PATH=$LIB_DIR/*

SPARK_JVM_OPTIONS=$($JAVA_HOME/bin/java -cp $JAR_PATH org.apache.gluten.integration.SparkJvmOptions)

EMBEDDED_SPARK_HOME=$BASEDIR/../spark-home

mkdir -p $EMBEDDED_SPARK_HOME && ln -snf ../package/target/lib $EMBEDDED_SPARK_HOME/jars

# We temporarily disallow setting these two variables by caller.
SPARK_HOME=""
SPARK_SCALA_VERSION=""
export SPARK_HOME=${SPARK_HOME:-$EMBEDDED_SPARK_HOME}
export SPARK_SCALA_VERSION=${SPARK_SCALA_VERSION:-'2.12'}

echo "SPARK_HOME set at [$SPARK_HOME]."
echo "SPARK_SCALA_VERSION set at [$SPARK_SCALA_VERSION]."

GLUTEN_IT_JVM_ARGS=${GLUTEN_IT_JVM_ARGS:-"-Xmx2G"}

$JAVA_HOME/bin/java \
    $SPARK_JVM_OPTIONS \
    $GLUTEN_IT_JVM_ARGS \
    -XX:ErrorFile=/var/log/java/hs_err_pid%p.log \
    -Dio.netty.tryReflectionSetAccessible=true \
    -cp $JAR_PATH \
    org.apache.gluten.integration.Cli "$@"

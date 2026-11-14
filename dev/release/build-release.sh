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

set -eu

CURRENT_DIR=$(cd "$(dirname "$BASH_SOURCE")"; pwd)
GLUTEN_HOME=${CURRENT_DIR}/../../
cd ${GLUTEN_HOME}

# Enable static build with support for S3, GCS, HDFS, and ABFS.
./dev/builddeps-veloxbe.sh --enable_vcpkg=ON --build_arrow=OFF --build_tests=OFF --build_benchmarks=OFF \
                           --build_examples=OFF --enable_s3=ON --enable_gcs=ON --enable_hdfs=ON --enable_abfs=ON

sudo yum install -y java-17-openjdk-devel
# Derive JAVA_HOME from the installed JDK 17 package instead of hard-coding the path,
# since the install location can vary across distros/packaging.
JAVAC_PATH=$(rpm -ql java-17-openjdk-devel | grep -m1 '/bin/javac$' || true)
if [[ -z "${JAVAC_PATH}" ]]; then
  echo "Error: Unable to locate javac from the java-17-openjdk-devel package."
  exit 1
fi
export JAVA_HOME=$(dirname "$(dirname "$(readlink -f "${JAVAC_PATH}")")")
export PATH=$JAVA_HOME/bin:$PATH

JAVA_VERSION=$("java" -version 2>&1 | awk -F '"' '/version/ {print $2}')
if [[ $JAVA_VERSION == 17* ]]; then
  echo "Java 17 is being used."
else
  echo "Error: Java 17 is required. Current version is $JAVA_VERSION."
  exit 1
fi

for spark_version in 3.4 3.5 4.0 4.1
do
  scala_profile=""
  # Spark 4.x is built with Scala 2.13; keep Spark 3.x behavior unchanged.
  if [[ "${spark_version}" == 4.* ]]; then
    scala_profile="-Pscala-2.13"
  fi

  ${GLUTEN_HOME}/build/mvn clean install -Pjava-17 -Pbackends-velox ${scala_profile} -Pbackends-velox -Pspark-${spark_version} -Pceleborn,uniffle \
                    -Piceberg,delta,hudi,paimon -DskipTests -Dmaven.source.skip
done

# partial support for spark-4.2
${GLUTEN_HOME}/build/mvn clean install -Pspark-4.2 -Pscala-2.13 -Pjava-17 -Pbackends-velox -Pceleborn -Puniffle -DskipTests -Dmaven.source.skip
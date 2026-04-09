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

source /opt/rh/devtoolset-11/enable
source /opt/rh/rh-git227/enable

CURRENT_DIR=$(cd "$(dirname "$BASH_SOURCE")"; pwd)
GLUTEN_HOME=${CURRENT_DIR}/../../
cd ${GLUTEN_HOME}

# Enable static build with support for S3, GCS, HDFS, and ABFS.
./dev/builddeps-veloxbe.sh --enable_vcpkg=ON --build_arrow=OFF --build_tests=OFF --build_benchmarks=OFF \
                           --build_examples=OFF --enable_s3=ON --enable_gcs=ON --enable_hdfs=ON --enable_abfs=ON

JAVA_VERSION=$("java" -version 2>&1 | awk -F '"' '/version/ {print $2}')

if [[ $JAVA_VERSION == 1.8* ]]; then
  echo "Java 8 is being used."
else
  echo "Error: Java 8 is required. Current version is $JAVA_VERSION."
  exit 1
fi

# Build Gluten for Spark 3.3 with Java 8. All feature modules are enabled.
for spark_version in 3.3
do
  ${GLUTEN_HOME}/build/mvn clean install -Pbackends-velox -Pspark-${spark_version} -Pceleborn,uniffle \
                    -Piceberg,delta,hudi,paimon -DskipTests
done

sudo curl -Lo /etc/yum.repos.d/corretto.repo https://yum.corretto.aws/corretto.repo
sudo yum install -y java-17-amazon-corretto-devel
export JAVA_HOME=/usr/lib/jvm/java-17-amazon-corretto
export PATH=$JAVA_HOME/bin:$PATH

JAVA_VERSION=$("java" -version 2>&1 | awk -F '"' '/version/ {print $2}')
if [[ $JAVA_VERSION == 17* ]]; then
  echo "Java 17 is being used."
else
  echo "Error: Java 17 is required. Current version is $JAVA_VERSION."
  exit 1
fi

# Build Gluten for Spark 3.4 and 3.5 with Java 17. The version of Iceberg being used requires Java 11 or higher.
# All feature modules are enabled.
for spark_version in 3.4 3.5
do
  ${GLUTEN_HOME}/build/mvn clean install -Pjava-17 -Pbackends-velox -Pspark-${spark_version} -Pceleborn,uniffle \
                    -Piceberg,delta,hudi,paimon -DskipTests
done

for spark_version in 4.0
do
  ${GLUTEN_HOME}/build/mvn clean install -Pjava-17 -Pscala-2.13 -Pbackends-velox -Pspark-${spark_version} -Piceberg -DskipTests
done

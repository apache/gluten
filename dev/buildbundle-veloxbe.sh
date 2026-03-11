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

BASEDIR=$(dirname "$0")
source "$BASEDIR/builddeps-veloxbe.sh"

# Use Gluten's Maven wrapper
MVN_CMD="${BASEDIR}/../build/mvn"

function build_for_spark {
  spark_version=$1
  # Extract major version (e.g., "3.2" -> "3", "4.0" -> "4")
  major_version=$(echo $spark_version | cut -d'.' -f1)

  if [ "$major_version" -ge 4 ]; then
    ${MVN_CMD} clean install -Pbackends-velox -Pspark-$spark_version -Pjava-17 -Pscala-2.13 -DskipTests
  else
    ${MVN_CMD} clean install -Pbackends-velox -Pspark-$spark_version -DskipTests
  fi
}

function check_supported {
  PLATFORM=$(${MVN_CMD} help:evaluate -Dexpression=platform -q -DforceStdout)
  ARCH=$(${MVN_CMD} help:evaluate -Dexpression=arch -q -DforceStdout)
  if [ "$PLATFORM" == "null object or invalid expression" ] || [ "$ARCH" == "null object or invalid expression" ]; then
    OS_NAME=$(${MVN_CMD} help:evaluate -Dexpression=os.name -q -DforceStdout)
    OS_ARCH=$(${MVN_CMD} help:evaluate -Dexpression=os.arch -q -DforceStdout)
    echo "$OS_NAME-$OS_ARCH is not supported by current Gluten build."
    exit 1
  fi
}

cd $GLUTEN_DIR

check_supported

# SPARK_VERSION is defined in builddeps-veloxbe.sh
# SUPPORTED_SPARK_VERSIONS array is also defined in builddeps-veloxbe.sh
if [ "$SPARK_VERSION" = "ALL" ]; then
  # Filter out "ALL" from the supported versions list
  for spark_version in "${SUPPORTED_SPARK_VERSIONS[@]}"
  do
    if [ "$spark_version" != "ALL" ]; then
      build_for_spark $spark_version
    fi
  done
else
  build_for_spark $SPARK_VERSION
fi

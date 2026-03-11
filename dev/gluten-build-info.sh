#!/bin/bash

#
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
#

GLUTEN_ROOT=$(cd $(dirname -- $0)/..; pwd -P)

EXTRA_RESOURCE_DIR=$GLUTEN_ROOT/gluten-core/target/generated-resources
BUILD_INFO="$EXTRA_RESOURCE_DIR"/gluten-build-info.properties

# Delete old build-info file before regenerating
rm -f "$BUILD_INFO"
mkdir -p "$EXTRA_RESOURCE_DIR"

function echo_revision_info() {
  echo branch=$(git rev-parse --abbrev-ref HEAD)
  echo revision=$(git rev-parse HEAD)
  echo revision_time=$(git show -s --format=%ci HEAD)
  echo date=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  echo url=$(git config --get remote.origin.url)
}

function echo_velox_revision_info() {
  BACKEND_HOME=$1
  echo gcc_version=$(strings $GLUTEN_ROOT/cpp/build/releases/libgluten.so | grep "GCC:" | head -n 1)
  echo velox_branch=$(git -C $BACKEND_HOME rev-parse --abbrev-ref HEAD)
  echo velox_revision=$(git -C $BACKEND_HOME rev-parse HEAD)
  echo velox_revision_time=$(git -C $BACKEND_HOME show -s --format=%ci HEAD)
}

function echo_clickhouse_revision_info() {
  echo ch_org=$(cat $GLUTEN_ROOT/cpp-ch/clickhouse.version | grep -oP '(?<=^CH_ORG=).*')
  echo ch_branch=$(cat $GLUTEN_ROOT/cpp-ch/clickhouse.version | grep -oP '(?<=^CH_BRANCH=).*')
  echo ch_commit=$(cat $GLUTEN_ROOT/cpp-ch/clickhouse.version | grep -oP '(?<=^CH_COMMIT=).*')
}

while (( "$#" )); do
  echo "$1"
  case $1 in
    --version)
      echo gluten_version="$2" >> "$BUILD_INFO"
      ;;
    --backend)
      BACKEND_TYPE="$2"
      echo backend_type="$BACKEND_TYPE" >> "$BUILD_INFO"
      # Compute backend home path based on type
      if [ "velox" = "$BACKEND_TYPE" ]; then
        BACKEND_HOME="$GLUTEN_ROOT/ep/build-velox/build/velox_ep"
        echo_velox_revision_info "$BACKEND_HOME" >> "$BUILD_INFO"
      elif [ "ch" = "$BACKEND_TYPE" ] || [ "clickhouse" = "$BACKEND_TYPE" ]; then
        echo_clickhouse_revision_info >> "$BUILD_INFO"
      fi
      ;;
    --java)
      echo java_version="$2" >> "$BUILD_INFO"
      ;;
    --scala)
      echo scala_version="$2" >> "$BUILD_INFO"
      ;;
    --spark)
      echo spark_version="$2" >> "$BUILD_INFO"
      ;;
    --hadoop)
      echo hadoop_version="$2" >> "$BUILD_INFO"
      ;;
    --revision)
      if [ "true" = "$2" ]; then
        echo_revision_info >> "$BUILD_INFO"
      fi
      ;;
    *)
      echo "Error: $1 is not supported"
      ;;
  esac
  shift 2
done

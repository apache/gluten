#!/usr/bin/env bash

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
BACKEND_HOME=""
BACKEND_TYPE=""
GLUTEN_VERSION=""
JAVA_VERSION=""
SCALA_VERSION=""
SPARK_VERSION=""
HADOOP_VERSION=""
WRITE_REVISION="false"

# Delete old build-info file before regenerating
rm -f "$BUILD_INFO"
mkdir -p "$EXTRA_RESOURCE_DIR"

function echo_revision_info() {
  echo branch=$(git rev-parse --abbrev-ref HEAD)
  echo revision=$(git rev-parse HEAD)
  echo revision_time=$(git show -s --format=%ci HEAD)
  echo date=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  # Strip embedded credentials (user:pass@) from the URL to avoid exposing them in the build info.
  echo url=$(git config --get remote.origin.url | sed 's|://[^:@]*:[^@]*@|://|')
}

function echo_velox_revision_info() {
  BACKEND_HOME=$1
  echo gcc_version=$(strings $GLUTEN_ROOT/cpp/build/releases/libgluten.so | grep "GCC:" | head -n 1)
  echo velox_branch=$(git -C "$BACKEND_HOME" rev-parse --abbrev-ref HEAD)
  echo velox_revision=$(git -C "$BACKEND_HOME" rev-parse HEAD)
  echo velox_revision_time=$(git -C "$BACKEND_HOME" show -s --format=%ci HEAD)
}

function echo_clickhouse_revision_info() {
  echo ch_org=$(cat $GLUTEN_ROOT/cpp-ch/clickhouse.version | grep -oP '(?<=^CH_ORG=).*')
  echo ch_branch=$(cat $GLUTEN_ROOT/cpp-ch/clickhouse.version | grep -oP '(?<=^CH_BRANCH=).*')
  echo ch_commit=$(cat $GLUTEN_ROOT/cpp-ch/clickhouse.version | grep -oP '(?<=^CH_COMMIT=).*')
}

function read_cmake_cache_path() {
  CACHE_FILE="$GLUTEN_ROOT/cpp/build/CMakeCache.txt"
  CACHE_KEY="$1"
  if [ -f "$CACHE_FILE" ]; then
    grep "^${CACHE_KEY}:PATH=" "$CACHE_FILE" | cut -d= -f2- | head -n 1
  fi
}

function resolve_velox_home() {
  if [ -n "$BACKEND_HOME" ]; then
    echo "$BACKEND_HOME"
    return
  fi

  if [ -n "${VELOX_HOME:-}" ]; then
    echo "$VELOX_HOME"
    return
  fi

  CACHED_VELOX_HOME=$(read_cmake_cache_path VELOX_HOME)
  if [ -n "$CACHED_VELOX_HOME" ]; then
    echo "$CACHED_VELOX_HOME"
    return
  fi

  echo "$GLUTEN_ROOT/ep/build-velox/build/velox_ep"
}

while (( "$#" )); do
  echo "$1"
  case $1 in
    --version)
      GLUTEN_VERSION="$2"
      ;;
    --backend)
      BACKEND_TYPE="$2"
      ;;
    --backend_home|--velox_home)
      if [ -n "$2" ]; then
        BACKEND_HOME="$2"
      fi
      ;;
    --java)
      JAVA_VERSION="$2"
      ;;
    --scala)
      SCALA_VERSION="$2"
      ;;
    --spark)
      SPARK_VERSION="$2"
      ;;
    --hadoop)
      HADOOP_VERSION="$2"
      ;;
    --revision)
      WRITE_REVISION="$2"
      ;;
    *)
      echo "Error: $1 is not supported"
      ;;
  esac
  shift 2
done

if [ -n "$GLUTEN_VERSION" ]; then
  echo gluten_version="$GLUTEN_VERSION" >> "$BUILD_INFO"
fi

if [ -n "$BACKEND_TYPE" ]; then
  echo backend_type="$BACKEND_TYPE" >> "$BUILD_INFO"
  if [ "velox" = "$BACKEND_TYPE" ]; then
    BACKEND_HOME=$(resolve_velox_home)
    echo_velox_revision_info "$BACKEND_HOME" >> "$BUILD_INFO"
  elif [ "ch" = "$BACKEND_TYPE" ] || [ "clickhouse" = "$BACKEND_TYPE" ]; then
    echo_clickhouse_revision_info >> "$BUILD_INFO"
  fi
fi

if [ -n "$JAVA_VERSION" ]; then
  echo java_version="$JAVA_VERSION" >> "$BUILD_INFO"
fi

if [ -n "$SCALA_VERSION" ]; then
  echo scala_version="$SCALA_VERSION" >> "$BUILD_INFO"
fi

if [ -n "$SPARK_VERSION" ]; then
  echo spark_version="$SPARK_VERSION" >> "$BUILD_INFO"
fi

if [ -n "$HADOOP_VERSION" ]; then
  echo hadoop_version="$HADOOP_VERSION" >> "$BUILD_INFO"
fi

if [ "true" = "$WRITE_REVISION" ]; then
  echo_revision_info >> "$BUILD_INFO"
fi

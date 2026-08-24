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

# Note: Manually create $GLUTEN_HOME/release/ and place the release JARs inside.
#       Provide the release tag (e.g., v1.5.0-rc0) as an argument to this script.

set -euo pipefail

usage() {
  echo "Usage: $0 <release_tag>  e.g., v1.5.0-rc0"
  exit 1
}

TAG="${1:-}"; [[ -n "$TAG" ]] || usage

TAG_VERSION=${TAG#v}

RELEASE_VERSION=${TAG_VERSION%-rc*}

CURRENT_DIR=$(cd "$(dirname "$BASH_SOURCE")"; pwd)
GLUTEN_HOME=${CURRENT_DIR}/../../
if [ ! -d "$GLUTEN_HOME/release/" ]; then
  echo "Release directory $GLUTEN_HOME/release/ does not exist."
  exit 1
fi

# The bundle JAR statically links third-party code, so per ASF policy the binary
# distribution must ship the LICENSE/NOTICE that cover it.
LICENSE_BINARY="${GLUTEN_HOME}/LICENSE-binary"
NOTICE_BINARY="${GLUTEN_HOME}/NOTICE-binary"
for f in "$LICENSE_BINARY" "$NOTICE_BINARY"; do
  if [[ ! -f "$f" ]]; then
    echo "Missing $f, required for the binary distribution."
    exit 1
  fi
done

pushd $GLUTEN_HOME/release/

SPARK_VERSIONS="3.3 3.4 3.5 4.0 4.1"

for v in $SPARK_VERSIONS; do
  # Spark 4.x requires Scala 2.13; the spark-4.x Maven profiles enforce it.
  if [[ "$v" == 4.* ]]; then
    SCALA="2.13"
  else
    SCALA="2.12"
  fi

  JAR="gluten-velox-bundle-spark${v}_${SCALA}-linux_amd64-${RELEASE_VERSION}.jar"

  if [[ ! -f "$JAR" ]]; then
    echo "Missing Gluten release JAR under $GLUTEN_HOME/release/ for Spark $v: $JAR"
    exit 1
  fi

  echo "Packaging for Spark $v (Scala $SCALA)..."
  # Stage a versioned top-level directory so extracting does not scatter files into the
  # current directory, and so LICENSE/NOTICE travel with the JAR.
  BIN_DIR="apache-gluten-${RELEASE_VERSION}-bin-spark-${v}"
  rm -rf "${BIN_DIR}"
  mkdir -p "${BIN_DIR}"
  cp "$JAR" "${BIN_DIR}/"
  cp "$LICENSE_BINARY" "${BIN_DIR}/LICENSE"
  cp "$NOTICE_BINARY" "${BIN_DIR}/NOTICE"
  tar -czf "${BIN_DIR}.tar.gz" "${BIN_DIR}"
  rm -rf "${BIN_DIR}"
done

SRC_ZIP="${TAG}.zip"
SRC_DIR="gluten-${RELEASE_VERSION}"

echo "Packaging source code..."
wget https://github.com/apache/gluten/archive/refs/tags/${SRC_ZIP}
unzip -q ${SRC_ZIP}

# Rename folder to remove "rc*" for formal release.
mv gluten-${TAG_VERSION} ${SRC_DIR}

# Remove .git and .github and other unwanted files from the source dir.
rm -rf ${SRC_DIR}/.git \
       ${SRC_DIR}/.github \
       ${SRC_DIR}/.gitattributes \
       ${SRC_DIR}/.gitignore \
       ${SRC_DIR}/.gitmodules \
       ${SRC_DIR}/.idea
rm -f "${SRC_DIR}/dev/vcpkg/.gitignore" \
      "${SRC_DIR}/gluten-uniffle/.gitkeep" \
      "${SRC_DIR}/tools/qualification-tool/.gitignore"

tar -czf apache-gluten-${RELEASE_VERSION}-src.tar.gz ${SRC_DIR}
rm -r ${SRC_ZIP} ${SRC_DIR}

popd

echo "Finished packaging release binaries and source code."

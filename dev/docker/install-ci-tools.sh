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

# Installs the tools that CI jobs used to download at runtime, so PR jobs do not depend on
# package mirrors. Distro packages are installed by each Dockerfile; this script adds:
#   - gh and jq, needed by the Apache Stash cache actions;
#   - with --pyspark-python=<python>, one virtualenv per PySpark version used by the Spark
#     unit tests, under /opt/pyspark/<version>. Jobs select one by putting its bin/ first in
#     PATH, which leaves the system python3 (and dnf) untouched.

set -ex

GH_VERSION=2.63.2
JQ_VERSION=1.7.1
PYSPARK_VERSIONS="3.4.4 3.5.5"
PYSPARK_PYTHON=""

for arg in "$@"; do
  case $arg in
  --pyspark-python=*)
    PYSPARK_PYTHON="${arg#*=}"
    ;;
  *)
    echo "Unknown argument: $arg" >&2
    exit 1
    ;;
  esac
done

case "$(uname -m)" in
  x86_64) ARCH=amd64 ;;
  aarch64) ARCH=arm64 ;;
  *) echo "unsupported arch: $(uname -m)" >&2; exit 1 ;;
esac

curl -fsSL "https://github.com/cli/cli/releases/download/v${GH_VERSION}/gh_${GH_VERSION}_linux_${ARCH}.tar.gz" \
  | tar -xz -C /usr/local --strip-components=1 "gh_${GH_VERSION}_linux_${ARCH}/bin/gh"
curl -fsSL -o /usr/local/bin/jq \
  "https://github.com/jqlang/jq/releases/download/jq-${JQ_VERSION}/jq-linux-${ARCH}"
chmod +x /usr/local/bin/jq
gh --version
jq --version

if [ -n "$PYSPARK_PYTHON" ]; then
  for version in $PYSPARK_VERSIONS; do
    venv=/opt/pyspark/$version
    "$PYSPARK_PYTHON" -m venv "$venv"
    "$venv/bin/pip" install --no-cache-dir --upgrade pip
    "$venv/bin/pip" install --no-cache-dir setuptools==77.0.3
    "$venv/bin/pip" install --no-cache-dir "pyspark==$version" cython
    "$venv/bin/pip" install --no-cache-dir pandas==2.2.3 pyarrow==20.0.0 regex requests
    "$venv/bin/python3" -c "import pyspark, pandas, pyarrow; print(pyspark.__version__)"
  done
fi

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

set -eux
set -o pipefail

GLUTEN_HOME=/PATH_TO_GLUTEN_HOME

# 1. Switch to gluten-it folder.
cd ${GLUTEN_HOME}/tools/gluten-it/
# 2. Set JAVA_HOME. For example, JDK 17.
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
# 3. Set JVM heap size. For example, 224G.
export GLUTEN_IT_JVM_ARGS="-Xmx224G"
# 4. Generate the tables.
sbin/gluten-it.sh \
  data-gen-only \
  --data-source=delta \
  --local \
  --benchmark-type=ds \
  --threads=112 \
  -s=100 \
  --gen-partitioned-data \
  --data-dir=/tmp/my-data

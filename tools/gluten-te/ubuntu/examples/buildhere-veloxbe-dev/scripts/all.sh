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

set -ex

# Build Gluten
EXTRA_MAVEN_OPTIONS="-Pspark-3.2 \
                     -Pbackends-velox \
                     -Pceleborn \
                     -Puniffle \
                     -DskipTests \
                     -Dscalastyle.skip=true \
                     -Dcheckstyle.skip=true"

cd /opt/gluten
bash dev/builddeps-veloxbe.sh --build_type=Debug
mvn clean install $EXTRA_MAVEN_OPTIONS

apt-get -y -q --no-install-recommends install firefox tmux openjdk-8-source

# Install IDEs
apt-get update
apt-get -y -q --no-install-recommends install libgbm-dev libxkbcommon-dev
mkdir -p /opt/ide
cd /opt/ide
wget https://download.jetbrains.com/idea/ideaIC-2022.3.2.tar.gz
tar -xvzf ideaIC-2022.3.2.tar.gz
wget https://download.jetbrains.com/cpp/CLion-2022.3.2.tar.gz
tar -xvzf CLion-2022.3.2.tar.gz

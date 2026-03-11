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

BASEDIR=$(dirname "$0")
# Use Gluten's Maven wrapper
MVN_CMD="${BASEDIR}/../build/mvn"

GLUTEN_UPDATE="${GLUTEN_UPDATE:-1}" \
${MVN_CMD} clean test \
  -Pbackends-velox -pl backends-velox -am \
  -Dtest=none \
  -DfailIfNoTests=false \
  -DwildcardSuites=org.apache.gluten.config.AllGlutenConfiguration,org.apache.gluten.config.AllVeloxConfiguration

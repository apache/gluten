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

BASEDIR=$(readlink -f $(dirname $0))

TIMESTAMP=$(date +%s)

# Set the following env to install Gluten's modified Arrow Jars on host.
export MOUNT_MAVEN_CACHE=ON
export EXTRA_DOCKER_OPTIONS="--name buildhere-veloxbe-portable-libs-$TIMESTAMP -v $BASEDIR/scripts:/opt/scripts"

BASH_ARGS="$*"

$BASEDIR/../../cbash-mount.sh "/opt/scripts/all.sh $BASH_ARGS"

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

BASEDIR=$(dirname $0)

source "$BASEDIR/shared.sh"

# Docker registry used to pull layer cache to speed-up builds
DOCKER_PULL_REGISTRY=${DOCKER_PULL_REGISTRY:-$DEFAULT_DOCKER_PULL_REGISTRY}

# Non-interactive during docker run
NON_INTERACTIVE=${NON_INTERACTIVE:-$DEFAULT_NON_INTERACTIVE}

# Do not remove stopped docker container
PRESERVE_CONTAINER=${PRESERVE_CONTAINER:-$DEFAULT_PRESERVE_CONTAINER}

# Docker options
EXTRA_DOCKER_OPTIONS=${EXTRA_DOCKER_OPTIONS:-$DEFAULT_EXTRA_DOCKER_OPTIONS}

# Whether to mount Maven cache
MOUNT_MAVEN_CACHE=${MOUNT_MAVEN_CACHE:-$DEFAULT_MOUNT_MAVEN_CACHE}

if [ -n "$DOCKER_PULL_REGISTRY" ]
then
  docker pull $DOCKER_PULL_REGISTRY/$DOCKER_TARGET_IMAGE_BUILD_WITH_OS_IMAGE
  docker tag $DOCKER_PULL_REGISTRY/$DOCKER_TARGET_IMAGE_BUILD_WITH_OS_IMAGE $DOCKER_TARGET_IMAGE_BUILD_WITH_OS_IMAGE
else
  $BASEDIR/build.sh
fi

CBASH_DOCKER_RUN_ARGS=
if [ "$NON_INTERACTIVE" != "ON" ]
then
  CBASH_DOCKER_RUN_ARGS="$CBASH_DOCKER_RUN_ARGS -it"
fi
if [ "$PRESERVE_CONTAINER" != "ON" ]
then
  CBASH_DOCKER_RUN_ARGS="$CBASH_DOCKER_RUN_ARGS --rm"
fi
CBASH_DOCKER_RUN_ARGS="$CBASH_DOCKER_RUN_ARGS --init"
CBASH_DOCKER_RUN_ARGS="$CBASH_DOCKER_RUN_ARGS --privileged"
CBASH_DOCKER_RUN_ARGS="$CBASH_DOCKER_RUN_ARGS --ulimit nofile=65536:65536"
CBASH_DOCKER_RUN_ARGS="$CBASH_DOCKER_RUN_ARGS --ulimit core=-1"
CBASH_DOCKER_RUN_ARGS="$CBASH_DOCKER_RUN_ARGS --security-opt seccomp=unconfined"
CBASH_DOCKER_RUN_ARGS="$CBASH_DOCKER_RUN_ARGS -e NUM_THREADS=$(nproc)"
if [ "$MOUNT_MAVEN_CACHE" == "ON" ]
then
  CBASH_DOCKER_RUN_ARGS="$CBASH_DOCKER_RUN_ARGS -v $HOME/.m2/repository:/root/.m2/repository"
fi
CBASH_DOCKER_RUN_ARGS="$CBASH_DOCKER_RUN_ARGS -v $HOME/.ccache:/root/.ccache"
CBASH_DOCKER_RUN_ARGS="$CBASH_DOCKER_RUN_ARGS $EXTRA_DOCKER_OPTIONS"

CBASH_BASH_ARGS="$*"
BASH_ARGS="$CBASH_BASH_ARGS"

docker run $CBASH_DOCKER_RUN_ARGS $DOCKER_TARGET_IMAGE_BUILD_WITH_OS_IMAGE "cd /opt/gluten && $BASH_ARGS"

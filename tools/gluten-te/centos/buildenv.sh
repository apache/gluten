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
DOCKER_CACHE_REGISTRY=${DOCKER_CACHE_REGISTRY:-$DEFAULT_DOCKER_CACHE_REGISTRY}

# Docker registry to push pre-built images
DOCKER_PUSH_REGISTRY=${DOCKER_PUSH_REGISTRY:-$DEFAULT_DOCKER_PUSH_REGISTRY}

# HTTP proxy
HTTP_PROXY_HOST=${HTTP_PROXY_HOST:-$DEFAULT_HTTP_PROXY_HOST}
HTTP_PROXY_PORT=${HTTP_PROXY_PORT:-$DEFAULT_HTTP_PROXY_PORT}

# If on, use maven mirror settings for PRC's network environment
USE_ALI_MAVEN_MIRROR=${USE_ALI_MAVEN_MIRROR:-$DEFAULT_USE_ALI_MAVEN_MIRROR}

# Whether to build Spark binaries in buildenv image
BUILD_SPARK_BINARIES=${BUILD_SPARK_BINARIES:-$DEFAULT_BUILD_SPARK_BINARIES}

# Set timezone name
TIMEZONE=${TIMEZONE:-$DEFAULT_TIMEZONE}


if [ "$USE_ALI_MAVEN_MIRROR" == "ON" ]
then
  MAVEN_MIRROR_URL='https://maven.aliyun.com/repository/public'
else
  MAVEN_MIRROR_URL=
fi

##

BUILDENV_DOCKER_BUILD_ARGS=

BUILDENV_DOCKER_BUILD_ARGS="$BUILDENV_DOCKER_BUILD_ARGS --ulimit nofile=8192:8192"
BUILDENV_DOCKER_BUILD_ARGS="$BUILDENV_DOCKER_BUILD_ARGS --build-arg BUILDKIT_INLINE_CACHE=1"
BUILDENV_DOCKER_BUILD_ARGS="$BUILDENV_DOCKER_BUILD_ARGS --build-arg TIMEZONE=$TIMEZONE"
BUILDENV_DOCKER_BUILD_ARGS="$BUILDENV_DOCKER_BUILD_ARGS --build-arg OS_IMAGE_NAME=$OS_IMAGE_NAME --build-arg OS_IMAGE_TAG=$OS_IMAGE_TAG"
BUILDENV_DOCKER_BUILD_ARGS="$BUILDENV_DOCKER_BUILD_ARGS --build-arg MAVEN_MIRROR_URL=$MAVEN_MIRROR_URL"
BUILDENV_DOCKER_BUILD_ARGS="$BUILDENV_DOCKER_BUILD_ARGS --build-arg HTTP_PROXY_HOST=$HTTP_PROXY_HOST"
BUILDENV_DOCKER_BUILD_ARGS="$BUILDENV_DOCKER_BUILD_ARGS --build-arg HTTP_PROXY_PORT=$HTTP_PROXY_PORT"
BUILDENV_DOCKER_BUILD_ARGS="$BUILDENV_DOCKER_BUILD_ARGS --build-arg BUILD_SPARK_BINARIES=$BUILD_SPARK_BINARIES"
BUILDENV_DOCKER_BUILD_ARGS="$BUILDENV_DOCKER_BUILD_ARGS -f $BASEDIR/dockerfile-buildenv"
BUILDENV_DOCKER_BUILD_ARGS="$BUILDENV_DOCKER_BUILD_ARGS --target gluten-buildenv"
BUILDENV_DOCKER_BUILD_ARGS="$BUILDENV_DOCKER_BUILD_ARGS -t $DOCKER_TARGET_IMAGE_BUILDENV_WITH_OS_IMAGE"

if [ -n "$DOCKER_CACHE_REGISTRY" ]
then
  BUILDENV_DOCKER_BUILD_ARGS="$BUILDENV_DOCKER_BUILD_ARGS --cache-from $DOCKER_CACHE_REGISTRY/$DOCKER_TARGET_IMAGE_BUILDENV_WITH_OS_IMAGE"
fi

BUILDENV_DOCKER_BUILD_ARGS="$BUILDENV_DOCKER_BUILD_ARGS $BASEDIR"

docker build $BUILDENV_DOCKER_BUILD_ARGS

if [ -n "$DOCKER_PUSH_REGISTRY" ]
then
  docker tag "$DOCKER_TARGET_IMAGE_BUILDENV_WITH_OS_IMAGE" "$DOCKER_PUSH_REGISTRY/$DOCKER_TARGET_IMAGE_BUILDENV_WITH_OS_IMAGE"
  docker push "$DOCKER_PUSH_REGISTRY/$DOCKER_TARGET_IMAGE_BUILDENV_WITH_OS_IMAGE"
fi

# EOF

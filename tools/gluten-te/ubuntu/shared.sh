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

SHARED_BASEDIR=$(dirname $0)

source "$SHARED_BASEDIR/defaults.conf"

# Enable buildkit
export DOCKER_BUILDKIT=1
export BUILDKIT_PROGRESS=plain

# Validate envs
if [ -z "$HOME" ]
then
  echo 'Environment variable $HOME not found. Aborting.'
  exit 1
fi

# Set operating system
OS_IMAGE_NAME=${OS_IMAGE_NAME:-$DEFAULT_OS_IMAGE_NAME}

# Set os version
OS_IMAGE_TAG=${OS_IMAGE_TAG:-$DEFAULT_OS_IMAGE_TAG}

# Buildenv will result in this image
DOCKER_TARGET_IMAGE_BUILDENV=${DOCKER_TARGET_IMAGE_BUILDENV:-$DEFAULT_DOCKER_TARGET_IMAGE_BUILDENV}

# Build will result in this image
DOCKER_TARGET_IMAGE_BUILD=${DOCKER_TARGET_IMAGE_BUILD:-$DEFAULT_DOCKER_TARGET_IMAGE_BUILD}

DOCKER_TARGET_IMAGE_BUILDENV_WITH_OS_IMAGE="$DOCKER_TARGET_IMAGE_BUILDENV-$OS_IMAGE_NAME:$OS_IMAGE_TAG"

DOCKER_TARGET_IMAGE_BUILD_WITH_OS_IMAGE="$DOCKER_TARGET_IMAGE_BUILD-$OS_IMAGE_NAME:$OS_IMAGE_TAG"

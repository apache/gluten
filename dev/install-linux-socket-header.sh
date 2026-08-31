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

set -euo pipefail

readonly LINUX_COMMIT="aad9c8c470f2a8321a99eb053630ce0e199558d6"
readonly SOCKET_HEADER_SHA256="6ca32f00f1c64a1b75886b868a9e51a47d74720e777e5d6a0df30291c179a691"
readonly SOCKET_HEADER_URL="https://raw.githubusercontent.com/torvalds/linux/${LINUX_COMMIT}/include/uapi/asm-generic/socket.h"
readonly SOCKET_HEADER_PATH="${SOCKET_HEADER_PATH:-/usr/include/asm-generic/socket.h}"

temp_file="$(mktemp)"
trap 'rm -f "${temp_file}"' EXIT

curl --fail --location --silent --show-error "${SOCKET_HEADER_URL}" --output "${temp_file}"
echo "${SOCKET_HEADER_SHA256}  ${temp_file}" | sha256sum --check --status
install --mode=0644 "${temp_file}" "${SOCKET_HEADER_PATH}"

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

set -eu
wrapper="$(cd "$(dirname "$0")"/../../../../.. && pwd)/build/mvn"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
cd "$tmp"
checkout=$'checkout\\name\nsecond line'
archive=apache-maven-0-bin.tar.gz
mkdir -p "$checkout/build/.mvn" apache-maven-0/bin
cp "$wrapper" "$checkout/build/mvn"
printf '<maven.version>0</maven.version>\n' > "$checkout/pom.xml"
printf '#!/bin/sh\npwd -P\n' > apache-maven-0/bin/mvn
chmod +x apache-maven-0/bin/mvn
tar -czf "$archive" apache-maven-0
if command -v sha512sum >/dev/null; then
    expected=$(sha512sum "$archive")
else
    expected=$(shasum -a 512 "$archive")
fi
export digest="${expected%% *}"
mv "$archive" "$checkout/build/.mvn/"

# The distribution is cached. Only the fallback checksum is available.
curl() {
    [[ "${!#}" = https://archive.apache.org/dist/*.sha512 ]] || return 1
    while [[ $# -gt 1 && "$1" != -o ]]; do shift; done
    [[ "$1" = -o ]] && printf '%s\n' "$digest" > "$2"
}
# Exercise checksum fallback without waiting through real retry delays.
sleep() { :; }
export -f curl sleep
caller="$(bash "$checkout/build/mvn")"
[[ "$caller" = "$(pwd -P)" ]]

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

export GLUTEN_SOURCE=$(builtin cd $(dirname $0)/../../..; pwd)
export CH_SOURCE_DIR=${GLUTEN_SOURCE}/cpp-ch/ClickHouse
CMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE:-RelWithDebInfo}

for arg in "$@"
do
    case $arg in
        -t=*|--src=*)
        GLUTEN_SOURCE=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        -s=*|--ch=*)
        CH_SOURCE_DIR=("${arg#*=}")
        shift # Remove argument name from processing
        ;;
        *)
        OTHER_ARGUMENTS+=("$1")
        shift # Remove generic argument from processing
        ;;
    esac
done

echo ${GLUTEN_SOURCE}

export CC=${CC:-clang-18}
export CXX=${CXX:-clang++-18}
cmake -G Ninja -S ${GLUTEN_SOURCE}/cpp-ch -B ${GLUTEN_SOURCE}/cpp-ch/build_ch -DCH_SOURCE_DIR=${CH_SOURCE_DIR} "-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}"
cmake --build ${GLUTEN_SOURCE}/cpp-ch/build_ch --target build_ch

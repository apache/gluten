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

CUR_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

set -e

function check_compiler() {
    if command -v gcc &> /dev/null; then
        gcc_version=$(gcc --version | grep -oP '(?<=^gcc \(GCC\) )\d+' | head -n 1)

        if [[ "$gcc_version" -eq 10 || "$gcc_version" -eq 11 || "$gcc_version" -eq 12 ]]; then
            echo "✅ gcc version：$gcc_version"
            return 0
        fi
    else
        echo "❌ gcc is not installed"
    fi

    echo "Bolt requires gcc-10/gcc-11/gcc-12/clang-16. Please install the correct compiler version."
    echo "Install complier by running(with root user): bash ${CUR_DIR}/install-gcc.sh"
    return 1
}

function check_conan() {
    if command -v conan &> /dev/null; then
        echo "✅ conan is installed"
    else
        if [ ! -d ~/miniconda3 ]; then
            echo "Installing conda"
            MINICONDA_VERSION=py310_23.1.0-1
            MINICONDA_URL=https://repo.anaconda.com/miniconda/Miniconda3-${MINICONDA_VERSION}-Linux-$(arch).sh
            wget --progress=dot:mega  ${MINICONDA_URL} -O /tmp/miniconda.sh
            chmod +x /tmp/miniconda.sh && /tmp/miniconda.sh -b -u -p ~/miniconda3 && rm -f /tmp/miniconda.sh
            echo "export PATH=~/miniconda3/bin:\$PATH" >> ~/.bashrc
            source ~/.bashrc && pip install --upgrade pip || true
        fi
        source ~/.bashrc
        pip install pydot && pip install requests
        pip install conan
    fi

    if [ -z "${CONAN_HOME:-}" ]; then
        export CONAN_HOME=~/.conan2
    fi

    if [ ! -f "${CONAN_HOME}/profiles/default" ]; then
        conan profile detect
    fi
    echo "Configuring conan profile to use gnu C++17 standard by default"
    sed -i 's/gnu14/gnu17/g' ${CONAN_HOME}/profiles/default
}

check_compiler
check_conan

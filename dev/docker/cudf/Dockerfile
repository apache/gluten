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

FROM ghcr.io/facebookincubator/velox-dev:adapters
ENV CUDA_ARCHITECTURES=70
ENV LD_LIBRARY_PATH=/opt/gluten/ep/build-velox/build/velox_ep/_build/release/_deps/curl-build/lib:$LD_LIBRARY_PATH


RUN yum install -y sudo patch perl && \
    dnf remove -y cuda-toolkit-12* && dnf install -y cuda-toolkit-13-1; \
    dnf autoremove -y && dnf clean all; \
    rm -rf /opt/rh/gcc-toolset-12 && ln -s /opt/rh/gcc-toolset-14 /opt/rh/gcc-toolset-12; \
    ln -sf /usr/local/bin/cmake /usr/bin && \
    git clone --depth=1 https://github.com/apache/incubator-gluten /opt/gluten && \
    cd /opt/gluten && \
    source /opt/rh/gcc-toolset-14/enable && \
    bash ./dev/buildbundle-veloxbe.sh --run_setup_script=OFF --build_arrow=ON --spark_version=3.4 --build_tests=ON --build_benchmarks=ON --enable_gpu=ON && \
    rm -rf /opt/gluten && \
    rm -rf /root/.cache/ccache
    

# You can try the data in folder backends-velox/src/test/resources/tpch-data-parquet


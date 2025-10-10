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

cmake_minimum_required(VERSION 3.25)

find_package(JNI REQUIRED)
find_package(glog CONFIG REQUIRED)

add_library(glutenlibloader SHARED JniNativeLibraryLoader.cc)
target_include_directories(glutenlibloader PUBLIC ${JNI_INCLUDE_DIRS})

target_link_libraries(glutenlibloader PRIVATE glog::glog)

# add arch suffix (.amd64 or .aarch64)
# Aligned with java ``` System.getProperty("os.arch") ```
IF((${CMAKE_SYSTEM_PROCESSOR} MATCHES "x86_64") OR (${CMAKE_SYSTEM_PROCESSOR} MATCHES "AMD64"))
  set(ARCH "amd64")
ELSEIF(${CMAKE_SYSTEM_PROCESSOR} MATCHES "aarch64")
  set(ARCH "aarch64")
ELSE()
  message(FATAL_ERROR "Unsupported System Architecture: ${CMAKE_SYSTEM_PROCESSOR}")
ENDIF()

set_target_properties(glutenlibloader PROPERTIES SUFFIX "${CMAKE_SHARED_LIBRARY_SUFFIX}")

install(TARGETS glutenlibloader
        DESTINATION ${CMAKE_CURRENT_SOURCE_DIR}/../../build/releases)
if(TARGET hdfs_client::hdfs_client)
    include(${hdfs_client_DIR}/hdfs_client-config-extras.cmake OPTIONAL)
    if(COMMAND install_hdfs_client_libraries)
        install_hdfs_client_libraries(${CMAKE_CURRENT_SOURCE_DIR}/../../build/releases)
    endif()
endif()


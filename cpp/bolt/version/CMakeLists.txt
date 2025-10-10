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

set(GLUTEN_BUILD_SHORT_HASH "unknown")
set(GLUTEN_BUILD_HASH "unknown")
set(GLUTEN_BUILD_DIRTY "false")
set(GLUTEN_MODIFIED_FILES "None")

find_package(Git)
if(GIT_FOUND)
set(FILE_TO_CHECK "${CMAKE_CURRENT_SOURCE_DIR}/CMakeLists.txt")
  execute_process(
    COMMAND git ls-files --error-unmatch ${FILE_TO_CHECK}
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    RESULT_VARIABLE git_result
    OUTPUT_VARIABLE git_output
    ERROR_QUIET
    OUTPUT_STRIP_TRAILING_WHITESPACE
  )

  if(git_result EQUAL 0)
    execute_process(COMMAND ${GIT_EXECUTABLE} rev-parse --short HEAD OUTPUT_VARIABLE SHORT_SHA OUTPUT_STRIP_TRAILING_WHITESPACE)
    execute_process(COMMAND ${GIT_EXECUTABLE} rev-parse HEAD OUTPUT_VARIABLE LONG_SHA OUTPUT_STRIP_TRAILING_WHITESPACE)
    set(GLUTEN_BUILD_SHORT_HASH ${SHORT_SHA})
    set(GLUTEN_BUILD_HASH ${LONG_SHA})
  else()
    message(WARNING "${FILE_TO_CHECK} is NOT under Git version control.")
  endif()
else()
  message(WARNING "Git not found, cannot set version info")
endif()

string(TIMESTAMP GLUTEN_BUILD_TIME "%Y-%m-%d %H:%M:%S")

cmake_host_system_information(RESULT _hostname QUERY HOSTNAME)
cmake_host_system_information(RESULT _proc_desc QUERY PROCESSOR_DESCRIPTION)
cmake_host_system_information(RESULT _platform QUERY OS_PLATFORM)
set(GLUTEN_BUILD_HOST ${_hostname})
set(GLUTEN_BUILD_PROCESSOR ${_proc_desc})
set(GLUTEN_BUILD_ARCH ${_platform})

if(CMAKE_VERSION VERSION_LESS "3.22.0") 
  cmake_host_system_information(RESULT _os_name QUERY OS_NAME)
else ()
  cmake_host_system_information(RESULT _os_name QUERY DISTRIB_PRETTY_NAME)
endif()
set(GLUTEN_BUILD_SYSTEM_NAME ${_os_name})
set(GLUTEN_BUILD_TYPE ${CMAKE_BUILD_TYPE})

# Check if there are uncommitted changes
execute_process(
  COMMAND ${GIT_EXECUTABLE} diff --quiet
  RESULT_VARIABLE GIT_DIFF_RESULT
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

if(NOT GIT_DIFF_RESULT EQUAL 0)
  set(GLUTEN_BUILD_DIRTY "true")

  # Get the list of modified files
  execute_process(
    COMMAND ${GIT_EXECUTABLE} diff --name-only
    OUTPUT_VARIABLE MODIFIED_FILES
    OUTPUT_STRIP_TRAILING_WHITESPACE
  )
  string(REPLACE "\n" ", " MODIFIED_FILES_CLEANED "${MODIFIED_FILES}")
  set(GLUTEN_MODIFIED_FILES ${MODIFIED_FILES_CLEANED})
endif()
# generate version.h
configure_file("${CMAKE_CURRENT_SOURCE_DIR}/version.h.in" "${CMAKE_CURRENT_SOURCE_DIR}/version.h" @ONLY)
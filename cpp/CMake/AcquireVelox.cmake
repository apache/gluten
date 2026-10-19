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

if(NOT DEFINED GLUTEN_HOME)
  get_filename_component(GLUTEN_HOME "${CMAKE_CURRENT_LIST_DIR}/../.." ABSOLUTE)
endif()
file(STRINGS "${CMAKE_CURRENT_LIST_DIR}/../../ep/build-velox/src/get-velox.sh"
     _velox_defaults REGEX "^VELOX_(REPO|BRANCH|ENHANCED_BRANCH)=")
foreach(_entry IN LISTS _velox_defaults)
  if(_entry MATCHES "^VELOX_([A-Z_]+)=(.+)$")
    set(_velox_default_${CMAKE_MATCH_1} "${CMAKE_MATCH_2}")
  endif()
endforeach()
if(NOT DEFINED VELOX_REPO OR VELOX_REPO STREQUAL "")
  set(VELOX_REPO "${_velox_default_REPO}" CACHE STRING "Velox Git repository" FORCE)
endif()
if(NOT DEFINED VELOX_BRANCH OR VELOX_BRANCH STREQUAL "")
  if(ENABLE_ENHANCED_FEATURES)
    set(_velox_ref "${_velox_default_ENHANCED_BRANCH}")
  else()
    set(_velox_ref "${_velox_default_BRANCH}")
  endif()
  set(VELOX_BRANCH "${_velox_ref}" CACHE STRING "Velox Git ref for new clones" FORCE)
endif()
if(NOT DEFINED VELOX_HOME OR VELOX_HOME STREQUAL "")
  set(VELOX_HOME "${GLUTEN_HOME}/ep/build-velox/build/velox_ep")
endif()
get_filename_component(VELOX_HOME "${VELOX_HOME}" ABSOLUTE BASE_DIR "${GLUTEN_HOME}")
set(VELOX_HOME "${VELOX_HOME}" CACHE PATH "Velox source checkout" FORCE)
if(EXISTS "${VELOX_HOME}" AND NOT IS_DIRECTORY "${VELOX_HOME}")
  message(FATAL_ERROR "VELOX_HOME is not a directory: ${VELOX_HOME}")
endif()
file(GLOB _velox_entries "${VELOX_HOME}/*")
if(NOT _velox_entries)
  find_package(Git REQUIRED)
  execute_process(
    COMMAND "${GIT_EXECUTABLE}" clone --branch "${VELOX_BRANCH}"
            --recurse-submodules "${VELOX_REPO}" "${VELOX_HOME}"
    RESULT_VARIABLE _velox_clone_result)
  if(NOT _velox_clone_result EQUAL 0)
    message(FATAL_ERROR
      "Velox clone failed (${_velox_clone_result}) at VELOX_HOME=${VELOX_HOME}. "
      "Inspect the partial checkout before retrying; it will not be overwritten.")
  endif()
endif()
foreach(_required CMakeLists.txt scripts/setup-helper-functions.sh)
  if(NOT EXISTS "${VELOX_HOME}/${_required}")
    message(FATAL_ERROR
      "VELOX_HOME=${VELOX_HOME} is missing ${_required}. "
      "Prepare the existing checkout manually; no files or refs were changed.")
  endif()
endforeach()
message(STATUS "Using Velox checkout: ${VELOX_HOME}")

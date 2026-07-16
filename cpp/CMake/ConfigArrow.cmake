# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set(ARROW_STATIC_LIBRARY_SUFFIX ".a")

set(ARROW_LIB_NAME "arrow")
set(ARROW_BUNDLED_DEPS "arrow_bundled_dependencies")

set(ARROW_INSTALL_DIR "${ARROW_HOME}/install")
set(ARROW_LIB_DIR "${ARROW_INSTALL_DIR}/lib")
set(ARROW_LIB64_DIR "${ARROW_INSTALL_DIR}/lib64")

function(FIND_ARROW_LIB LIB_NAME)
  if(NOT TARGET Arrow::${LIB_NAME})
    set(ARROW_LIB_FULL_NAME
        ${CMAKE_SHARED_LIBRARY_PREFIX}${LIB_NAME}${ARROW_STATIC_LIBRARY_SUFFIX})
    add_library(Arrow::${LIB_NAME} STATIC IMPORTED)
    # Firstly find the lib from bundled path in Velox. If not found, try to find
    # it from system.
    find_library(
      ARROW_LIB_${LIB_NAME}
      NAMES ${ARROW_LIB_FULL_NAME}
      PATHS ${ARROW_LIB_DIR} ${ARROW_LIB64_DIR}
      NO_DEFAULT_PATH)
    if(NOT ARROW_LIB_${LIB_NAME})
      find_library(ARROW_LIB_${LIB_NAME} NAMES ${ARROW_LIB_FULL_NAME})
    endif()
    if(NOT ARROW_LIB_${LIB_NAME})
      message(FATAL_ERROR "Arrow library Not Found: ${ARROW_LIB_FULL_NAME}")
    endif()
    message(STATUS "Found Arrow library: ${ARROW_LIB_${LIB_NAME}}")

    # Walk up from the library directory until we find a parent that contains an
    # "include/arrow/api.h" anchor file.  This handles both conventional layouts
    # (/usr/local/lib/libarrow.a → /usr/local/include) and multi-arch ones
    # (/usr/lib/x86_64-linux-gnu/libarrow.a → /usr/include) without hard-coding
    # the number of directory levels to strip.
    get_filename_component(_arrow_search_dir "${ARROW_LIB_${LIB_NAME}}" PATH)
    set(ARROW_LIB_INCLUDE_DIR "")
    foreach(_level RANGE 5)
      if(EXISTS "${_arrow_search_dir}/include/arrow/api.h")
        set(ARROW_LIB_INCLUDE_DIR "${_arrow_search_dir}/include")
        break()
      endif()
      get_filename_component(_arrow_search_dir "${_arrow_search_dir}" PATH)
    endforeach()
    if(NOT ARROW_LIB_INCLUDE_DIR)
      message(
        FATAL_ERROR
          "Could not locate Arrow headers near ${ARROW_LIB_${LIB_NAME}}. "
          "Set ARROW_HOME to the Arrow installation prefix (the directory "
          "that contains lib/ and include/).")
    endif()
    message(STATUS "Found Arrow include: ${ARROW_LIB_INCLUDE_DIR}")

    set_target_properties(
      Arrow::${LIB_NAME}
      PROPERTIES IMPORTED_LOCATION ${ARROW_LIB_${LIB_NAME}}
                 INTERFACE_INCLUDE_DIRECTORIES ${ARROW_LIB_INCLUDE_DIR})
  endif()
endfunction()

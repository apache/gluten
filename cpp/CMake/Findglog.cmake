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

set(GLUTEN_GLOG_MINIMUM_VERSION 0.4.0)
set(GLUTEN_GLOG_VERSION 0.6.0)

if(NOT BUILD_GLOG)
  include(FindPackageHandleStandardArgs)
  include(SelectLibraryConfigurations)

  find_library(GLOG_LIBRARY_RELEASE glog PATHS ${GLOG_LIBRARYDIR})
  find_library(GLOG_LIBRARY_DEBUG glogd PATHS ${GLOG_LIBRARYDIR})

  find_path(GLOG_INCLUDE_DIR glog/logging.h PATHS ${GLOG_INCLUDEDIR})

  select_library_configurations(GLOG)

  find_package_handle_standard_args(glog DEFAULT_MSG GLOG_LIBRARY
                                    GLOG_INCLUDE_DIR)

  mark_as_advanced(GLOG_LIBRARY GLOG_INCLUDE_DIR)
endif()

if(NOT glog_FOUND)
  include(BuildGlog)
endif()

if(NOT TARGET glog::glog)
  get_filename_component(libglog_ext ${GLOG_LIBRARY} EXT)
  if(libglog_ext STREQUAL ".a")
    set(libglog_type STATIC)
    set(libgflags_component static)
  else()
    set(libglog_type SHARED)
    set(libgflags_component shared)
  endif()

  add_library(glog::glog ${libglog_type} IMPORTED)
  set_target_properties(glog::glog PROPERTIES INTERFACE_INCLUDE_DIRECTORIES
                                              "${GLOG_INCLUDE_DIR}")
  set_target_properties(
    glog::glog PROPERTIES IMPORTED_LINK_INTERFACE_LANGUAGES "C"
                          IMPORTED_LOCATION "${GLOG_LIBRARY}")

  set(GLUTEN_GFLAGS_VERSION 2.2.2)
  find_package(gflags ${GLUTEN_GFLAGS_VERSION} CONFIG
               COMPONENTS ${libgflags_component})

  if(NOT gflags_FOUND AND glog_FOUND)
    message(
      FATAL_ERROR
        "Glog found but Gflags not found. Set BUILD_GLOG=ON and reload cmake.")
  endif()

  if(gflags_FOUND)
    if(NOT TARGET gflags::gflags_${libgflags_component}
       AND NOT TARGET gflags_${libgflags_component})
      message(
        FATAL_ERROR
          "Found Gflags but missing component gflags_${libgflags_component}")
    endif()
    if(TARGET gflags::gflags_${libgflags_component})
      set_target_properties(
        glog::glog PROPERTIES IMPORTED_LINK_INTERFACE_LIBRARIES
                              gflags::gflags_${libgflags_component})
    else()
      set_target_properties(
        glog::glog PROPERTIES IMPORTED_LINK_INTERFACE_LIBRARIES
                              gflags_${libgflags_component})
    endif()
  else()
    include(BuildGflags)
    set_target_properties(
      glog::glog PROPERTIES IMPORTED_LINK_INTERFACE_LIBRARIES gflags_static)
  endif()
endif()

# Licensed to the Apache Software Foundation (ASF) under one or more contributor
# license agreements.  See the NOTICE file distributed with this work for
# additional information regarding copyright ownership. The ASF licenses this
# file to You under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License.  You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

set(_roaring_pkgconfig_hints "")
set(_roaring_include_hints "")
set(_roaring_library_hints "")

foreach(_root ${VELOX_BUILD_PATH} ${VELOX_HOME}/_build/release
              ${VELOX_HOME}/_build/debug)
  if(_root)
    list(APPEND _roaring_pkgconfig_hints "${_root}/_deps/roaring-build")
    list(APPEND _roaring_include_hints "${_root}/_deps/roaring-src/include"
         "${_root}/_deps/roaring-src/cpp"
         "${_root}/_deps/roaring-build/include")
    list(APPEND _roaring_library_hints "${_root}/_deps/roaring-build/src"
         "${_root}/_deps/roaring-build")
  endif()
endforeach()

if(DEFINED VELOX_HOME)
  list(APPEND _roaring_pkgconfig_hints
       "${VELOX_HOME}/deps-install/lib/pkgconfig"
       "${VELOX_HOME}/deps-install/lib64/pkgconfig")
  list(APPEND _roaring_include_hints "${VELOX_HOME}/deps-install/include")
  list(APPEND _roaring_library_hints "${VELOX_HOME}/deps-install/lib"
       "${VELOX_HOME}/deps-install/lib64")
endif()

list(REMOVE_DUPLICATES _roaring_pkgconfig_hints)
list(REMOVE_DUPLICATES _roaring_include_hints)
list(REMOVE_DUPLICATES _roaring_library_hints)

function(_gluten_roaring_add_cpp_headers target_name)
  find_path(
    Roaring_CPP_INCLUDE_DIR
    NAMES roaring/roaring64map.hh
    HINTS ${_roaring_include_hints})
  if(Roaring_CPP_INCLUDE_DIR)
    target_include_directories(${target_name}
                               INTERFACE "${Roaring_CPP_INCLUDE_DIR}")
  endif()
endfunction()

# Check if roaring target already exists.
if(TARGET roaring)
  _gluten_roaring_add_cpp_headers(roaring)
  message(STATUS "Target roaring was already found.")
  return()
endif()

find_package(PkgConfig QUIET)

if(PkgConfig_FOUND)
  set(_roaring_saved_pkg_config_path "$ENV{PKG_CONFIG_PATH}")
  list(JOIN _roaring_pkgconfig_hints ":" _roaring_hint_pkg_config_path)
  if(_roaring_hint_pkg_config_path)
    if(_roaring_saved_pkg_config_path STREQUAL "")
      set(ENV{PKG_CONFIG_PATH} "${_roaring_hint_pkg_config_path}")
    else()
      set(ENV{PKG_CONFIG_PATH}
          "${_roaring_hint_pkg_config_path}:${_roaring_saved_pkg_config_path}")
    endif()
  endif()
  pkg_check_modules(Roaring QUIET IMPORTED_TARGET roaring)
  set(ENV{PKG_CONFIG_PATH} "${_roaring_saved_pkg_config_path}")
endif()

if(Roaring_FOUND)
  add_library(roaring INTERFACE)
  target_link_libraries(roaring INTERFACE PkgConfig::Roaring)
  _gluten_roaring_add_cpp_headers(roaring)
  message(STATUS "Found roaring via pkg-config.")
  return()
endif()

find_path(
  Roaring_INCLUDE_DIR
  NAMES roaring/roaring.h
  HINTS ${_roaring_include_hints})
find_path(
  Roaring_CPP_INCLUDE_DIR
  NAMES roaring/roaring64map.hh
  HINTS ${_roaring_include_hints})
find_library(
  Roaring_LIBRARY
  NAMES roaring
  HINTS ${_roaring_library_hints})

if(Roaring_INCLUDE_DIR
   AND Roaring_CPP_INCLUDE_DIR
   AND Roaring_LIBRARY)
  add_library(roaring UNKNOWN IMPORTED)
  set_target_properties(
    roaring
    PROPERTIES IMPORTED_LOCATION "${Roaring_LIBRARY}"
               INTERFACE_INCLUDE_DIRECTORIES
               "${Roaring_INCLUDE_DIR};${Roaring_CPP_INCLUDE_DIR}")
  _gluten_roaring_add_cpp_headers(roaring)
  message(STATUS "Found roaring via direct library lookup.")
  return()
endif()

include(FetchContent)

set(_roaring_version "4.3.11")
set(_roaring_sha256
    "acd3a17e6c95acf3154d60c38c90e927e6c531805bc740a040fa111819af380e")
set(ENABLE_ROARING_TESTS
    OFF
    CACHE BOOL "" FORCE)

FetchContent_Declare(
  roaring_fetch
  URL "https://github.com/RoaringBitmap/CRoaring/archive/refs/tags/v${_roaring_version}.tar.gz"
  URL_HASH "SHA256=${_roaring_sha256}")
FetchContent_MakeAvailable(roaring_fetch)

if(TARGET roaring)
  _gluten_roaring_add_cpp_headers(roaring)
  message(STATUS "Found roaring via FetchContent.")
  return()
endif()

if(Roaring_FIND_REQUIRED)
  message(
    FATAL_ERROR
      "Failed to find roaring. Set VELOX_HOME/VELOX_BUILD_PATH to a Velox build "
      "that contains roaring in _deps, or provide roaring via PKG_CONFIG_PATH.")
elseif(NOT Roaring_FIND_QUIETLY)
  message(WARNING "Failed to find roaring.")
endif()

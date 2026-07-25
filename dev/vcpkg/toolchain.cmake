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

# This file will be used by cmake before cmake function `project(xxx)`
# is executed, even though it's an external cmake project.

set(ENABLE_GLUTEN_VCPKG ON)

# Force the use of VCPKG classic mode to avoid reinstalling vcpkg features during building
# different CMake sub-projects. Which means, the features installed by `vcpkg install`
# in script `init.sh` will be used across all CMake sub-projects.
#
# Reference: https://learn.microsoft.com/en-us/vcpkg/users/buildsystems/cmake-integration
#
# Note: "CACHE BOOL" is required to make this successfully override the option defined in
# vcpkg.cmake.
set(VCPKG_MANIFEST_MODE OFF CACHE BOOL "Use manifest mode, as opposed to classic mode." FORCE)

set(VCPKG_TARGET_TRIPLET $ENV{VCPKG_TRIPLET})
set(VCPKG_HOST_TRIPLET $ENV{VCPKG_TRIPLET})
set(VCPKG_INSTALLED_DIR $ENV{VCPKG_MANIFEST_DIR}/vcpkg_installed)
set(VCPKG_INSTALL_OPTIONS --no-print-usage)

set(CMAKE_FIND_ROOT_PATH_MODE_PACKAGE ONLY)
include($ENV{VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake)

if(DEFINED ENV{GLUTEN_VCPKG_PREFER_CONFIG}
   AND NOT "$ENV{GLUTEN_VCPKG_PREFER_CONFIG}" STREQUAL "")
  set(_GLUTEN_VCPKG_PREFER_CONFIG "$ENV{GLUTEN_VCPKG_PREFER_CONFIG}")
elseif(DEFINED CMAKE_FIND_PACKAGE_PREFER_CONFIG)
  set(_GLUTEN_VCPKG_PREFER_CONFIG "${CMAKE_FIND_PACKAGE_PREFER_CONFIG}")
else()
  set(_GLUTEN_VCPKG_PREFER_CONFIG ON)
endif()
set(CMAKE_FIND_PACKAGE_PREFER_CONFIG "${_GLUTEN_VCPKG_PREFER_CONFIG}" CACHE BOOL
    "Prefer package configuration files." FORCE)
unset(_GLUTEN_VCPKG_PREFER_CONFIG)
set(CMAKE_FIND_ROOT_PATH_MODE_PACKAGE ONLY CACHE STRING "Search for packages only in root paths." FORCE)
set(CMAKE_FIND_USE_PACKAGE_REGISTRY OFF CACHE BOOL "Disable the user package registry." FORCE)
set(CMAKE_FIND_USE_SYSTEM_PACKAGE_REGISTRY OFF CACHE BOOL "Disable the system package registry." FORCE)

set(_GLUTEN_VCPKG_IGNORED_PREFIXES /usr/local)
if(APPLE)
  list(APPEND _GLUTEN_VCPKG_IGNORED_PREFIXES /opt/homebrew /opt/local)
endif()
if(NOT "$ENV{CONDA_PREFIX}" STREQUAL "")
  list(APPEND _GLUTEN_VCPKG_IGNORED_PREFIXES "$ENV{CONDA_PREFIX}")
endif()
list(APPEND CMAKE_IGNORE_PREFIX_PATH ${_GLUTEN_VCPKG_IGNORED_PREFIXES})
list(REMOVE_DUPLICATES CMAKE_IGNORE_PREFIX_PATH)
set(CMAKE_IGNORE_PREFIX_PATH "${CMAKE_IGNORE_PREFIX_PATH}" CACHE STRING
    "Prefixes ignored by Gluten's vcpkg toolchain." FORCE)
unset(_GLUTEN_VCPKG_IGNORED_PREFIXES)

set(CMAKE_EXE_LINKER_FLAGS "-static-libstdc++ -static-libgcc")
set(CMAKE_SHARED_LINKER_FLAGS "-static-libstdc++ -static-libgcc")

# Disable boost new version warning for FindBoost module
set(Boost_NO_WARN_NEW_VERSIONS ON)

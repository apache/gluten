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

foreach(
  _header IN
  ITEMS "${GLUTEN_PAIMON_LIBCXX_INCLUDE}/__config_site"
        "${GLUTEN_PAIMON_LIBCXX_SOURCE_INCLUDE}/string"
        "${GLUTEN_PAIMON_LIBCXXABI_INCLUDE}/cxxabi.h")
  if(NOT EXISTS "${_header}")
    message(FATAL_ERROR "ClickHouse C++ header was not generated: ${_header}")
  endif()
endforeach()

foreach(
  _archive IN
  ITEMS "${GLUTEN_PAIMON_LIBCXX_ARCHIVE}" "${GLUTEN_PAIMON_LIBCXXABI_ARCHIVE}"
        "${GLUTEN_PAIMON_LIBUNWIND_ARCHIVE}"
        "${GLUTEN_PAIMON_BUILTINS_ARCHIVE}")
  if(NOT EXISTS "${_archive}")
    message(FATAL_ERROR "ClickHouse runtime archive was not built: ${_archive}")
  endif()
endforeach()

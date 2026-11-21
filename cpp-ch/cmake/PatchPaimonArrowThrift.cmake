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

# Adapt upstream paimon-cpp's CMake files before configuring its build. Bundle
# its dependencies and plugins into libpaimon.so, link ClickHouse's libc++
# runtime, and add selected glibc compatibility wrappers. Paimon keeps its
# upstream Arrow version despite this script's historical name.
set(_toolchain "${PAIMON_SOURCE_DIR}/cmake_modules/ThirdpartyToolchain.cmake")
file(READ "${_toolchain}" _contents)
set(_original_contents "${_contents}")
if(NOT _contents MATCHES "-DThrift_SOURCE=BUNDLED")
  set(_anchor "        -DARROW_DEPENDENCY_USE_SHARED=OFF")
  set(_replacement "        -DThrift_SOURCE=BUNDLED\n${_anchor}")
  string(FIND "${_contents}" "${_anchor}" _position)
  if(_position EQUAL -1)
    message(
      FATAL_ERROR
        "Cannot enable bundled Thrift in ${_toolchain}: Arrow CMake arguments changed"
    )
  endif()
  string(REPLACE "${_anchor}" "${_replacement}" _contents "${_contents}")
endif()
if(NOT _contents MATCHES "-DWITH_UNWIND=none[ \n]+-DWITH_GFLAGS=OFF")
  set(_anchor "        -DWITH_GFLAGS=OFF")
  set(_replacement "        -DWITH_UNWIND=none\n${_anchor}")
  string(FIND "${_contents}" "${_anchor}" _position)
  if(_position EQUAL -1)
    message(
      FATAL_ERROR
        "Cannot disable glog libunwind in ${_toolchain}: glog CMake arguments changed"
    )
  endif()
  string(REPLACE "${_anchor}" "${_replacement}" _contents "${_contents}")
endif()
if(_contents MATCHES "GLUTEN_PAIMON_EXE_LINKER_FLAGS")
  string(
    REPLACE
      "\"-DCMAKE_EXE_LINKER_FLAGS=\${GLUTEN_PAIMON_EXE_LINKER_FLAGS}\""
      "\"-DCMAKE_CXX_STANDARD_LIBRARIES=\${GLUTEN_PAIMON_CXX_STANDARD_LIBRARIES}\""
      _contents
      "${_contents}")
elseif(NOT _contents MATCHES "GLUTEN_PAIMON_CXX_STANDARD_LIBRARIES")
  set(_anchor "    -DCMAKE_CXX_FLAGS=\${EP_CXX_FLAGS}")
  set(_replacement
      "${_anchor}\n    \"-DCMAKE_CXX_STANDARD_LIBRARIES=\${GLUTEN_PAIMON_CXX_STANDARD_LIBRARIES}\""
  )
  string(FIND "${_contents}" "${_anchor}" _position)
  if(_position EQUAL -1)
    message(
      FATAL_ERROR
        "Cannot propagate ClickHouse libc++ linker flags: ${_toolchain} changed"
    )
  endif()
  string(REPLACE "${_anchor}" "${_replacement}" _contents "${_contents}")
endif()
string(
  FIND "${_contents}"
       "\"-DCMAKE_CXX_STANDARD_LIBRARIES=\${GLUTEN_PAIMON_CXX_STANDARD_LIBRARIES}\"\n    -DCMAKE_EXE_LINKER_FLAGS="
       _linker_flags_position)
if(_linker_flags_position EQUAL -1)
  string(
    REPLACE
      "\"-DCMAKE_CXX_STANDARD_LIBRARIES=\${GLUTEN_PAIMON_CXX_STANDARD_LIBRARIES}\""
      "\"-DCMAKE_CXX_STANDARD_LIBRARIES=\${GLUTEN_PAIMON_CXX_STANDARD_LIBRARIES}\"\n    -DCMAKE_EXE_LINKER_FLAGS="
      _contents
      "${_contents}")
endif()
if(NOT _contents STREQUAL _original_contents)
  file(WRITE "${_toolchain}" "${_contents}")
endif()

# Keep the main Paimon shared library and fold its plugin object files into it.
# The factories register themselves through static initializers, so the whole
# object libraries must be included even if no symbols are referenced directly.
set(_build_utils "${PAIMON_SOURCE_DIR}/cmake_modules/BuildUtils.cmake")
file(READ "${_build_utils}" _contents)
set(_original_contents "${_contents}")
set(_dependency_anchor "            if(TARGET \${_paimon_mapped_dep})")
set(_dependency_replacement
    "            if(TARGET \${_paimon_mapped_dep} AND NOT (NOT LIB_NAME STREQUAL \"paimon\" AND _paimon_mapped_dep STREQUAL \"paimon_shared\"))"
)
if(NOT _contents MATCHES "_paimon_mapped_dep STREQUAL \\\"paimon_shared\\\"")
  string(FIND "${_contents}" "${_dependency_anchor}" _position)
  if(_position EQUAL -1)
    message(
      FATAL_ERROR
        "Cannot remove Paimon plugin dependency cycle: ${_build_utils} changed")
  endif()
  string(REPLACE "${_dependency_anchor}" "${_dependency_replacement}" _contents
                 "${_contents}")
endif()
set(_anchor "    if(ARG_BUILD_STATIC)\n")
set(_replacement
    "    if(NOT LIB_NAME STREQUAL \"paimon\")\n        set(BUILD_SHARED OFF)\n    endif()\n${_anchor}"
)
if(NOT _contents MATCHES "if\\(NOT LIB_NAME STREQUAL \\\"paimon\\\"\\)")
  string(FIND "${_contents}" "${_anchor}" _position)
  if(_position EQUAL -1)
    message(
      FATAL_ERROR
        "Cannot configure a single Paimon shared library: ${_build_utils} changed"
    )
  endif()
  string(REPLACE "${_anchor}" "${_replacement}" _contents "${_contents}")
endif()
if(NOT _contents STREQUAL _original_contents)
  file(WRITE "${_build_utils}" "${_contents}")
endif()

set(_root_cmake "${PAIMON_SOURCE_DIR}/CMakeLists.txt")
file(READ "${_root_cmake}" _contents)
set(_original_contents "${_contents}")
set(_anchor "add_subdirectory(test/inte)")
set(_replacement
    [=[
add_subdirectory(test/inte)

# Gluten deploys one Paimon DSO.  Include every enabled plugin's object files
# in the main library, preserving their static factory registrations.
foreach(_plugin IN ITEMS
        paimon_file_index paimon_global_index paimon_local_file_system
        paimon_blob_file_format paimon_orc_file_format
        paimon_parquet_file_format paimon_avro_file_format
        paimon_s3_file_system paimon_jindo_file_system
        paimon_lucene_index paimon_lumina_index paimon_tantivy_index)
    if(TARGET ${_plugin}_objlib)
        target_sources(paimon_shared PRIVATE $<TARGET_OBJECTS:${_plugin}_objlib>)
    endif()
endforeach()
target_link_libraries(paimon_shared PRIVATE parquet)
if(PAIMON_ENABLE_ORC)
    target_link_libraries(paimon_shared PRIVATE orc::orc)
endif()
if(PAIMON_ENABLE_AVRO)
    target_link_libraries(paimon_shared PRIVATE avro)
endif()
# Keep the C++ runtime self-contained; executors need only libpaimon.so.
target_link_options(paimon_shared PRIVATE -nostdlib++)
target_link_libraries(paimon_shared PRIVATE -Wl,--whole-archive
    "${GLUTEN_PAIMON_LIBCXX_ARCHIVE}"
    -Wl,--no-whole-archive
    "${GLUTEN_PAIMON_LIBCXXABI_ARCHIVE}"
    "${GLUTEN_PAIMON_LIBUNWIND_ARCHIVE}")
]=])
if(NOT _contents MATCHES "Gluten deploys one Paimon DSO")
  string(FIND "${_contents}" "${_anchor}" _position)
  if(_position EQUAL -1)
    message(
      FATAL_ERROR
        "Cannot add Paimon plugins to the main library: ${_root_cmake} changed")
  endif()
  string(REPLACE "${_anchor}" "${_replacement}" _contents "${_contents}")
endif()
if(NOT _contents MATCHES
   "target_link_options\\(paimon_shared PRIVATE -nostdlib\\+\\+\\)")
  if(NOT GLUTEN_PAIMON_LIBCXX_ARCHIVE
     OR NOT GLUTEN_PAIMON_LIBCXXABI_ARCHIVE
     OR NOT GLUTEN_PAIMON_LIBUNWIND_ARCHIVE)
    message(
      FATAL_ERROR "ClickHouse C++ runtime archives are required to link Paimon")
  endif()
  set(_anchor "target_link_libraries(paimon_shared PRIVATE avro)\nendif()")
  set(_replacement
      "${_anchor}\n# Keep the C++ runtime self-contained; executors need only libpaimon.so.\ntarget_link_options(paimon_shared PRIVATE -nostdlib++)\ntarget_link_libraries(paimon_shared PRIVATE -Wl,--whole-archive\n    \"\${GLUTEN_PAIMON_LIBCXX_ARCHIVE}\"\n    -Wl,--no-whole-archive\n    \"\${GLUTEN_PAIMON_LIBCXXABI_ARCHIVE}\"\n    \"\${GLUTEN_PAIMON_LIBUNWIND_ARCHIVE}\")"
  )
  string(FIND "${_contents}" "${_anchor}" _position)
  if(_position EQUAL -1)
    message(
      FATAL_ERROR
        "Cannot link LLVM C++ runtime into ${_root_cmake}: Paimon CMake changed"
    )
  endif()
  string(REPLACE "${_anchor}" "${_replacement}" _contents "${_contents}")
endif()
# Bind math calls to their original glibc versions and emulate the newer pthread
# clock wait call, including references from bundled static archives.
if(NOT GLUTEN_PAIMON_COMPAT_SOURCE OR NOT EXISTS
                                      "${GLUTEN_PAIMON_COMPAT_SOURCE}")
  message(
    FATAL_ERROR "GLUTEN_PAIMON_COMPAT_SOURCE must name the compatibility source"
  )
endif()
set(_compat_anchor "target_link_options(paimon_shared PRIVATE -nostdlib++)")
set(_compat_symbols
    exp
    exp2
    expf
    getenv
    log
    log2
    log2f
    logf
    pow
    powf
    pthread_cond_clockwait)
if(NOT _contents MATCHES "GLUTEN_PAIMON_COMPAT_SOURCE")
  set(_compat_options "")
  foreach(_symbol IN LISTS _compat_symbols)
    string(APPEND _compat_options " -Wl,--wrap=${_symbol}")
  endforeach()
  set(_compat_replacement
      "${_compat_anchor}\ntarget_sources(paimon_shared PRIVATE \"\${GLUTEN_PAIMON_COMPAT_SOURCE}\")\ntarget_link_options(paimon_shared PRIVATE${_compat_options})"
  )
  string(FIND "${_contents}" "${_compat_anchor}" _compat_position)
  if(_compat_position EQUAL -1)
    message(
      FATAL_ERROR
        "Cannot add glibc compatibility to ${_root_cmake}: Paimon CMake changed"
    )
  endif()
  string(REPLACE "${_compat_anchor}" "${_compat_replacement}" _contents
                 "${_contents}")
endif()
# Also update a source tree patched by an earlier version of this script.
foreach(_symbol IN LISTS _compat_symbols)
  if(NOT _contents MATCHES "--wrap=${_symbol}([ )]|$)")
    set(_anchor " -Wl,--wrap=exp")
    string(FIND "${_contents}" "${_anchor}" _position)
    if(_position EQUAL -1)
      message(
        FATAL_ERROR
          "Cannot wrap ${_symbol}: Paimon link options changed in ${_root_cmake}"
      )
    endif()
    string(REPLACE "${_anchor}" " -Wl,--wrap=${_symbol}${_anchor}" _contents
                   "${_contents}")
  endif()
endforeach()
foreach(
  _symbol IN
  ITEMS malloc
        free
        calloc
        realloc
        memalign
        aligned_alloc
        posix_memalign)
  string(REPLACE " -Wl,--wrap=${_symbol}" "" _contents "${_contents}")
endforeach()
string(REGEX
       REPLACE "\"[^\"]*/libc\\+\\+\\.a\""
               "\"\${GLUTEN_PAIMON_LIBCXX_ARCHIVE}\"" _contents "${_contents}")
string(
  REGEX
  REPLACE "\"[^\"]*/libc\\+\\+abi\\.a\""
          "\"\${GLUTEN_PAIMON_LIBCXXABI_ARCHIVE}\"" _contents "${_contents}")
string(
  REGEX
  REPLACE "\"[^\"]*/libunwind\\.a\"" "\"\${GLUTEN_PAIMON_LIBUNWIND_ARCHIVE}\""
          _contents "${_contents}")
if(NOT _contents STREQUAL _original_contents)
  file(WRITE "${_root_cmake}" "${_contents}")
endif()
# These two implementation files are already compiled into paimon_objlib.
set(_global_index_cmake
    "${PAIMON_SOURCE_DIR}/src/paimon/common/global_index/CMakeLists.txt")
file(READ "${_global_index_cmake}" _contents)
set(_anchor "add_paimon_lib(paimon_global_index")
set(_replacement
    "list(REMOVE_ITEM PAIMON_GLOBAL_INDEX_SRC\n    offset_global_index_reader.cpp union_global_index_reader.cpp)\n\n${_anchor}"
)
if(NOT _contents MATCHES "list\\(REMOVE_ITEM PAIMON_GLOBAL_INDEX_SRC")
  string(FIND "${_contents}" "${_anchor}" _position)
  if(_position EQUAL -1)
    message(
      FATAL_ERROR
        "Cannot deduplicate Paimon index sources: ${_global_index_cmake} changed"
    )
  endif()
  string(REPLACE "${_anchor}" "${_replacement}" _contents "${_contents}")
  file(WRITE "${_global_index_cmake}" "${_contents}")
endif()

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

# Reuse the loaded backend engine. Linking bolt::bolt into a plugin creates
# another static LLVM registry; the backend target also exposes static archives
# through its link interface, so use its shared binary and compile requirements.
add_library(myudf SHARED "MyUDF.cc")
target_link_libraries(myudf PRIVATE "$<TARGET_FILE:${PROJECT_NAME}>"
                                    folly::folly fmt::fmt glog::glog)
add_dependencies(myudf ${PROJECT_NAME})
target_compile_definitions(
  myudf PRIVATE "$<TARGET_PROPERTY:${PROJECT_NAME},COMPILE_DEFINITIONS>")
target_include_directories(
  myudf PRIVATE ${CMAKE_SOURCE_DIR}/bolt ${CMAKE_SOURCE_DIR}/core
                "$<TARGET_PROPERTY:${PROJECT_NAME},INCLUDE_DIRECTORIES>")

add_library(myudaf SHARED "MyUDAF.cc")
target_link_libraries(myudaf PRIVATE "$<TARGET_FILE:${PROJECT_NAME}>"
                                     folly::folly fmt::fmt glog::glog)
add_dependencies(myudaf ${PROJECT_NAME})
target_compile_definitions(
  myudaf PRIVATE "$<TARGET_PROPERTY:${PROJECT_NAME},COMPILE_DEFINITIONS>")
target_include_directories(
  myudaf PRIVATE ${CMAKE_SOURCE_DIR}/bolt ${CMAKE_SOURCE_DIR}/core
                 "$<TARGET_PROPERTY:${PROJECT_NAME},INCLUDE_DIRECTORIES>")

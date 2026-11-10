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

add_test_case(round_robin_partitioner_test SOURCES RoundRobinPartitionerTest.cc)
add_test_case(object_store_test SOURCES ObjectStoreTest.cc)
add_test_case(memory_allocator_test SOURCES MemoryAllocatorTest.cc)
add_test_case(ffor_codec_test SOURCES FForCodecTest.cc)
add_test_case(print_config_test SOURCES PrintConfigTest.cc)
add_test_case(payload_test SOURCES PayloadTest.cc)
add_test_case(java_exception_test SOURCES JavaExceptionTest.cc)
add_test_case(java_exception_allocation_test SOURCES
              JavaExceptionAllocationTest.cc)

find_package(Java REQUIRED COMPONENTS Runtime Development)
include(UseJava)

add_jar(
  java_exception_jni_test_jar
  SOURCES
    java/org/apache/gluten/jni/JavaExceptionJniTest.java
    ${GLUTEN_HOME}/gluten-core/src/main/java/org/apache/gluten/exception/GlutenException.java
    ${GLUTEN_HOME}/gluten-arrow/src/main/java/org/apache/gluten/exception/JniExceptionDescriber.java
)
get_target_property(JAVA_EXCEPTION_TEST_JAR java_exception_jni_test_jar
                    JAR_FILE)

add_library(java_exception_jni_test SHARED JavaExceptionJniTest.cc)
target_link_libraries(java_exception_jni_test
                      PRIVATE gluten_core_impl glog::glog Threads::Threads)
add_dependencies(java_exception_jni_test java_exception_jni_test_jar)

foreach(CASE direct nested nativeOnly normalReturn)
  add_test(
    NAME java_exception_jni_test.${CASE}
    COMMAND
      ${Java_JAVA_EXECUTABLE} -Xcheck:jni -Xmx64m -cp
      ${JAVA_EXCEPTION_TEST_JAR} org.apache.gluten.jni.JavaExceptionJniTest
      $<TARGET_FILE:java_exception_jni_test> ${CASE})
  set_tests_properties(java_exception_jni_test.${CASE} PROPERTIES TIMEOUT 30)
endforeach()

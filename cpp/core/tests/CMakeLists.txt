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
add_test_case(
  jni_thread_attachment_test SOURCES
  ${GLUTEN_HOME}/gluten-ut/native/JniThreadAttachmentTest.cc EXTRA_LINK_LIBS
  ${JAVA_JVM_LIBRARY})

# Hadoop's JNI libhdfs is optional, and distinct from the native libhdfs3. At
# runtime export CLASSPATH="$(hadoop classpath --glob)"; no NameNode is needed.
if(ENABLE_HDFS)
  find_path(HADOOP_JNI_INCLUDE_DIR hdfs.h HINTS "$ENV{HADOOP_HOME}/include")
  find_library(HADOOP_JNI_LIBRARY hdfs HINTS "$ENV{HADOOP_HOME}/lib/native")
  if(HADOOP_JNI_INCLUDE_DIR AND HADOOP_JNI_LIBRARY)
    add_test_case(
      jni_hdfs_thread_cleanup_test
      SOURCES
      ${GLUTEN_HOME}/gluten-ut/native/JniHdfsThreadCleanupTest.cc
      EXTRA_INCLUDES
      ${HADOOP_JNI_INCLUDE_DIR}
      EXTRA_LINK_LIBS
      ${JAVA_JVM_LIBRARY}
      ${HADOOP_JNI_LIBRARY})
  else()
    message(STATUS "JNI HDFS cleanup test requires Hadoop's libhdfs and hdfs.h")
  endif()
endif()

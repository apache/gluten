/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "JniTest.h"
#include "jni/JniThreadAttachment.h"

#include <fcntl.h>
#include <gtest/gtest.h>
#include <hdfs.h>
#include <pthread.h>
#include <unistd.h>
#include <cstdlib>
#include <thread>

#if !defined(__linux__) || !defined(__GLIBC__)
TEST(JniHdfsThreadCleanupTest, requiresGlibcThreadExitOrdering) {
  GTEST_SKIP() << "Automatic JNI thread-exit cleanup is only enabled on Linux/glibc";
}
#else

namespace gluten {
namespace {

using JniHdfsThreadCleanup = JniTest;

TEST_F(JniHdfsThreadCleanup, localFilesCloseBeforeOwnedAttachmentIsReleased) {
  if (std::getenv("CLASSPATH") == nullptr) {
    GTEST_SKIP() << "Set CLASSPATH to the Hadoop client jars (hadoop classpath --glob)";
  }

  char path[] = "/tmp/gluten-jni-hdfs-XXXXXX";
  const int fd = mkstemp(path);
  ASSERT_GE(fd, 0);
  EXPECT_EQ(close(fd), 0);
  struct FileCleanup {
    hdfsFS fs = nullptr;
    hdfsFile file = nullptr;
    bool ran = false;
  };
  // Install the file cleanup before libhdfs first creates its own TLS key, as
  // with a Folly ThreadLocal that closes a cached HdfsFile on worker exit.
  pthread_key_t fileKey;
  ASSERT_EQ(
      pthread_key_create(
          &fileKey,
          [](void* value) {
            auto* cleanup = static_cast<FileCleanup*>(value);
            cleanup->ran = true;
            EXPECT_EQ(hdfsCloseFile(cleanup->fs, cleanup->file), 0);
            EXPECT_EQ(hdfsDisconnect(cleanup->fs), 0);
          }),
      0);

  for (bool glutenAttachesFirst : {true, false}) {
    SCOPED_TRACE(glutenAttachesFirst);
    FileCleanup cleanup;
    jobject thread = nullptr;
    auto worker = std::thread(withJniThreadLifecycle([&] {
      JNIEnv* workerEnv = nullptr;
      if (glutenAttachesFirst) {
        ASSERT_EQ(getOrAttachCurrentThreadAsDaemon(vm_, &workerEnv), JNI_OK);
      }
      // Uses the real libhdfs JNI/TLS implementation, but only a local file://
      // filesystem: no NameNode, credentials or network service is required.
      cleanup.fs = hdfsConnect("file:///", 0);
      ASSERT_NE(cleanup.fs, nullptr);
      cleanup.file = hdfsOpenFile(cleanup.fs, path, O_RDONLY, 0, 0, 0);
      ASSERT_NE(cleanup.file, nullptr);
      if (!glutenAttachesFirst) {
        ASSERT_EQ(getOrAttachCurrentThreadAsDaemon(vm_, &workerEnv), JNI_OK);
      }
      thread = captureThread(workerEnv);
      ASSERT_EQ(pthread_setspecific(fileKey, &cleanup), 0);
    }));
    worker.join();
    EXPECT_TRUE(cleanup.ran);
    checkExited(thread);
  }
  EXPECT_EQ(pthread_key_delete(fileKey), 0);
  EXPECT_EQ(unlink(path), 0);
}

} // namespace
} // namespace gluten

#endif

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

#include <gtest/gtest.h>
#include <pthread.h>

#include <thread>

#include "jni/JniCommon.h"
#include "tests/utils/TestJniEnvironment.h"

using namespace gluten;

namespace {

JavaVM* vmOf(TestJniEnvironment& jni) {
  JavaVM* vm = nullptr;
  jni.env()->GetJavaVM(&vm);
  return vm;
}

void glutenAttach(JavaVM* vm) {
  JNIEnv* env = nullptr;
  attachCurrentThreadAsDaemonOrThrow(vm, &env);
  ASSERT_NE(env, nullptr);
}

// Mirrors libhdfs: it caches the thread's JNIEnv* in a pthread key and, from that key's destructor
// at thread exit, still uses the JNIEnv* and then detaches the thread (hdfsThreadDestructor).
struct HdfsLikeState {
  TestJniEnvironment* jni{nullptr};
  bool attachedWhenDestructorRan{false};
};

void hdfsLikeDestructor(void* value) {
  auto* state = static_cast<HdfsLikeState*>(value);
  state->attachedWhenDestructorRan = state->jni->workerAttached;
  vmOf(*state->jni)->DetachCurrentThread();
}

pthread_key_t createHdfsLikeKey() {
  pthread_key_t key;
  EXPECT_EQ(pthread_key_create(&key, hdfsLikeDestructor), 0);
  return key;
}

// Created during static initialization, so before Gluten's key: its destructor runs first in each round.
const pthread_key_t hdfsKeyCreatedBeforeGluten = createHdfsLikeKey();

void runHdfsAfterGlutenAttach(pthread_key_t hdfsKey) {
  TestJniEnvironment jni;
  HdfsLikeState state{&jni};
  std::thread worker([&] {
    glutenAttach(vmOf(jni));
    pthread_setspecific(hdfsKey, &state); // libhdfs then uses the already-attached thread.
  });
  worker.join();
  EXPECT_TRUE(state.attachedWhenDestructorRan); // libhdfs never sees a detached thread.
  EXPECT_EQ(jni.attaches, 1);
  EXPECT_EQ(jni.detaches, 1); // only libhdfs detached it.
  EXPECT_FALSE(jni.workerAttached);
}

} // namespace

TEST(JniThreadExitTest, detachesThreadAttachedByGlutenOnExit) {
  TestJniEnvironment jni;
  std::thread worker([&] { glutenAttach(vmOf(jni)); });
  worker.join();
  EXPECT_EQ(jni.attaches, 1);
  EXPECT_EQ(jni.detaches, 1);
  EXPECT_FALSE(jni.workerAttached);
}

TEST(JniThreadExitTest, doesNotDetachThreadAttachedElsewhere) {
  TestJniEnvironment jni;
  jni.workerAttached = true; // e.g. a Java-created thread, or one Java or libhdfs attached first.
  std::thread worker([&] { glutenAttach(vmOf(jni)); });
  worker.join();
  EXPECT_EQ(jni.attaches, 0);
  EXPECT_EQ(jni.detaches, 0);
  EXPECT_TRUE(jni.workerAttached);
}

TEST(JniThreadExitTest, detachesOnceWhenHelperIsCalledRepeatedly) {
  TestJniEnvironment jni;
  std::thread worker([&] {
    for (int i = 0; i < 3; ++i) {
      glutenAttach(vmOf(jni));
    }
  });
  worker.join();
  EXPECT_EQ(jni.attaches, 1);
  EXPECT_EQ(jni.detaches, 1);
}

TEST(JniThreadExitTest, detachesReattachedThreadOnExit) {
  TestJniEnvironment jni;
  std::thread worker([&] {
    glutenAttach(vmOf(jni));
    vmOf(jni)->DetachCurrentThread();
    glutenAttach(vmOf(jni));
  });
  worker.join();
  EXPECT_EQ(jni.attaches, 2);
  EXPECT_EQ(jni.detaches, 2); // one explicit detach, one at thread exit.
  EXPECT_FALSE(jni.workerAttached);
}

TEST(JniThreadExitTest, libhdfsDestructorRunsFirstWhenItsKeyIsOlder) {
  runHdfsAfterGlutenAttach(hdfsKeyCreatedBeforeGluten);
}

TEST(JniThreadExitTest, libhdfsDestructorRunsFirstWhenItsKeyIsNewer) {
  {
    // Make sure Gluten's key exists before creating the libhdfs-like key.
    TestJniEnvironment jni;
    std::thread worker([&] { glutenAttach(vmOf(jni)); });
    worker.join();
  }
  runHdfsAfterGlutenAttach(createHdfsLikeKey());
}

TEST(JniThreadExitTest, leavesThreadAttachedByLibhdfsToLibhdfs) {
  TestJniEnvironment jni;
  HdfsLikeState state{&jni};
  std::thread worker([&] {
    JNIEnv* env = nullptr;
    vmOf(jni)->AttachCurrentThreadAsDaemon(reinterpret_cast<void**>(&env), nullptr); // libhdfs attaches.
    pthread_setspecific(hdfsKeyCreatedBeforeGluten, &state);
    glutenAttach(vmOf(jni)); // Gluten then uses it; already attached, so nothing is registered.
  });
  worker.join();
  EXPECT_TRUE(state.attachedWhenDestructorRan);
  EXPECT_EQ(jni.attaches, 1);
  EXPECT_EQ(jni.detaches, 1);
}

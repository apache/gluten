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

// Native JNI helpers for Java integration tests (backends-velox/src/test/java).
// Separated from VeloxJniWrapper.cc to keep production code clean.

#include <jni.h>

#include <atomic>
#include <memory>
#include <thread>

#include <jni/JniCommon.h>
#include <jni/JniError.h>

#include "compute/Runtime.h"

#ifdef __cplusplus
extern "C" {
#endif

// Regression test helper for JniThreadDetachTest.
//
// Reproduces the exact production crash on a native std::thread (CPUThreadPool):
//   1. Attach thread, save env (simulates libhdfs caching JNIEnv in TLS)
//   2. Create and destroy a real JniColumnarBatchIterator (destructor under test)
//   3. Reuse saved env for FindClass (simulates libhdfs's next hdfsGetPathInfo)
//
// With the fix (no DetachCurrentThread): step 3 succeeds, returns true.
// With the bug (DetachCurrentThread present): step 3 triggers SIGSEGV — JVM crashes.
JNIEXPORT jboolean JNICALL
Java_org_apache_gluten_jni_JniThreadDetachTest_nativeTestIteratorDestructorKeepsThreadAttached( // NOLINT
    JNIEnv* env,
    jclass,
    jlong runtimeHandle,
    jobject jColumnarBatchItr) {
  JNI_METHOD_START
  JavaVM* vm;
  if (env->GetJavaVM(&vm) != JNI_OK) {
    throw gluten::GlutenException("Unable to get JavaVM instance");
  }
  auto* runtime = reinterpret_cast<gluten::Runtime*>(runtimeHandle);

  // Convert local ref to global ref so the native thread can use it.
  jobject globalItr = env->NewGlobalRef(jColumnarBatchItr);

  std::atomic<bool> succeeded{false};

  // Spawn a native thread (simulates CPUThreadPool).
  std::thread t([vm, runtime, globalItr, &succeeded]() {
    // Step 1: Attach and save env.
    // In production, libhdfs does this and caches env in __thread TLS.
    JNIEnv* savedEnv = nullptr;
    attachCurrentThreadAsDaemonOrThrow(vm, &savedEnv);

    // Step 2: Create and destroy a real JniColumnarBatchIterator.
    // The destructor previously called DetachCurrentThread, invalidating savedEnv.
    {
      auto iter = std::make_unique<gluten::JniColumnarBatchIterator>(savedEnv, globalItr, runtime);
      // Real destructor runs here.
    }

    // Step 3: Reuse savedEnv — simulates libhdfs returning TLS-cached env.
    // With the bug: savedEnv is stale, FindClass triggers SIGSEGV (JVM crashes).
    // With the fix: savedEnv is valid, FindClass succeeds.
    jclass cls = savedEnv->FindClass("java/lang/String");
    if (cls != nullptr) {
      savedEnv->DeleteLocalRef(cls);
    }

    succeeded.store(true);
  });
  t.join();

  env->DeleteGlobalRef(globalItr);
  return succeeded.load() ? JNI_TRUE : JNI_FALSE;
  JNI_METHOD_END(JNI_FALSE)
}

#ifdef __cplusplus
}
#endif

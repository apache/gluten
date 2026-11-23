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

#include "jni/JniThreadExit.h"

#include <pthread.h>

#include <atomic>
#include <cstdint>

#include <glog/logging.h>

namespace gluten {
namespace {

// The TSD value of a thread whose detach is pending. A destructor that finds kArmed has run in the
// first destructor round; it re-arms with kDeferred so that it is called again in the next round.
constexpr uintptr_t kArmed = 1;
constexpr uintptr_t kDeferred = 2;

// The VM to detach from. The destructor only receives the TSD value (kArmed or kDeferred), so it reads
// the VM from here. Every registration stores the same pointer, since a process has one JVM; the
// atomic only avoids a data race between registering threads and exiting threads.
std::atomic<JavaVM*> exitDetachVm{nullptr};
pthread_key_t exitDetachKey;

// Why the detach waits one destructor round:
//
// libhdfs caches the JNIEnv* of each thread it uses and detaches that thread from its own TSD
// destructor, which still dereferences the cached JNIEnv*. If this destructor detached first, libhdfs
// would use a freed JNIEnv* (the crash fixed by GLUTEN-11895, which is why Gluten stopped detaching
// in its JNI helpers). POSIX re-runs TSD destructors for keys whose values are non-NULL again after a
// round, and glibc does so for up to PTHREAD_DESTRUCTOR_ITERATIONS (4) rounds. Deferring by one round
// means libhdfs's destructor has run, whatever its key's position, before this one acts: the thread
// is then already detached (GetEnv returns JNI_EDETACHED and nothing is done) or libhdfs never owned
// it and this destructor detaches it.
//
// Detaching from a TSD destructor is supported by HotSpot: its own TSD destructor restores the
// current-thread pointer so that later destructors can still call DetachCurrentThread.
//
// A C++ thread_local destructor cannot replace this key: glibc runs all thread_local destructors
// before any pthread TSD destructor, so the detach would always precede libhdfs's destructor and
// bring back the GLUTEN-11895 crash for threads that libhdfs uses.
void detachOnThreadExit(void* value) {
  if (reinterpret_cast<uintptr_t>(value) == kArmed) {
    pthread_setspecific(exitDetachKey, reinterpret_cast<void*>(kDeferred));
    return;
  }
  JavaVM* vm = exitDetachVm.load(std::memory_order_acquire);
  JNIEnv* env = nullptr;
  // JNI_VERSION_1_8 is the JNI interface version requested from GetEnv, the same one Gluten uses
  // everywhere; every JDK from 8 on supports it, so this check applies to all JDKs. It only asks
  // whether the thread is still attached: JNI_EDETACHED means libhdfs already detached it.
  if (vm != nullptr && vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION_1_8) == JNI_OK) {
    vm->DetachCurrentThread();
  }
}

bool createExitDetachKey() {
  const int rc = pthread_key_create(&exitDetachKey, detachOnThreadExit);
  if (rc != 0) {
    LOG(WARNING) << "pthread_key_create failed with " << rc
                 << "; native threads will stay attached to the JVM after they exit";
  }
  return rc == 0;
}

} // namespace

void detachCurrentThreadOnExit(JavaVM* vm) {
  static const bool keyCreated = createExitDetachKey();
  if (!keyCreated) {
    return;
  }
  exitDetachVm.store(vm, std::memory_order_release);
  pthread_setspecific(exitDetachKey, reinterpret_cast<void*>(kArmed));
}

} // namespace gluten

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

#include "jni/JniThreadAttachment.h"

#include <limits.h>
#include <pthread.h>
#include <memory>
#include <stdexcept>
#include <system_error>

namespace gluten {

#if defined(__linux__) && defined(__GLIBC__)
namespace {

struct Attachment {
  pthread_key_t key;
  JavaVM* ownedVm = nullptr;
  int remainingPasses = PTHREAD_DESTRUCTOR_ITERATIONS;
};

// These are trivial TLS values: they remain usable during pthread destructors.
// Keep one implementation in libgluten rather than one TLS key per backend DSO.
thread_local Attachment* currentAttachment = nullptr;
thread_local bool cleanupFinished = false;

void destroyAttachment(void* value) {
  std::unique_ptr<Attachment> attachment(static_cast<Attachment*>(value));
  // Folly's thread-local deleters and libhdfs's TLS cleanup still use JNI. Give
  // them the preceding pthread destructor passes; do not detach in C++ TLS
  // destruction, or when a task/iterator finishes on a reusable worker.
  // The key is installed at thread entry, even for workers that first attach
  // from an exit callback, so the countdown starts in the first destructor pass.
  if (--attachment->remainingPasses > 0) {
    if (pthread_setspecific(attachment->key, attachment.get()) == 0) {
      attachment.release();
      return;
    }
    // Re-arming an already allocated key should not fail. In that exceptional
    // case, do not invalidate an env that another TLS destructor can still use.
    currentAttachment = nullptr;
    cleanupFinished = true;
    return;
  }
  currentAttachment = nullptr;
  cleanupFinished = true;
  void* env = nullptr;
  auto* vm = attachment->ownedVm;
  if (vm != nullptr && vm->GetEnv(&env, JNI_VERSION_1_8) == JNI_OK) {
    // Another library (or an explicit detach) may have cleaned up first.
    vm->DetachCurrentThread();
  }
}

pthread_key_t attachmentKey() {
  static const pthread_key_t key = [] {
    pthread_key_t value;
    const auto error = pthread_key_create(&value, destroyAttachment);
    if (error != 0) {
      throw std::system_error(error, std::generic_category(), "Creating native JNI thread key");
    }
    return value;
  }();
  return key;
}

} // namespace

void initializeNativeThreadJni() {
  if (currentAttachment != nullptr) {
    return;
  }
  if (cleanupFinished) {
    throw std::logic_error("Cannot register JNI lifecycle after native thread cleanup");
  }
  auto attachment = std::make_unique<Attachment>(Attachment{attachmentKey()});
  const auto error = pthread_setspecific(attachment->key, attachment.get());
  if (error != 0) {
    throw std::system_error(error, std::generic_category(), "Registering native JNI thread cleanup");
  }
  currentAttachment = attachment.release();
}

#else

// Other libcs may reclaim compiler/JVM TLS during the pthread destructor
// passes (for example, Darwin). Preserve their existing attachment behavior;
// the glibc cleanup order must not be assumed to be portable.
void initializeNativeThreadJni() {}

#endif

namespace {

jint getOrAttachCurrentThreadImpl(JavaVM* vm, JNIEnv** out, bool daemon) {
  const auto status = vm->GetEnv(reinterpret_cast<void**>(out), JNI_VERSION_1_8);
  if (status != JNI_EDETACHED) {
    return status;
  }
#if defined(__linux__) && defined(__GLIBC__)
  if (cleanupFinished) {
    // No destructor pass remains to release a new attachment. Fail rather than
    // silently leaving a terminated worker registered in the JVM.
    return JNI_ERR;
  }
#endif
  const auto attachStatus = daemon ? vm->AttachCurrentThreadAsDaemon(reinterpret_cast<void**>(out), nullptr)
                                   : vm->AttachCurrentThread(reinterpret_cast<void**>(out), nullptr);
#if defined(__linux__) && defined(__GLIBC__)
  if (attachStatus == JNI_OK && currentAttachment != nullptr) {
    currentAttachment->ownedVm = vm;
  }
#endif
  return attachStatus;
}

} // namespace

jint getOrAttachCurrentThreadAsDaemon(JavaVM* vm, JNIEnv** out) {
  return getOrAttachCurrentThreadImpl(vm, out, true);
}

jint getOrAttachCurrentThread(JavaVM* vm, JNIEnv** out) {
  return getOrAttachCurrentThreadImpl(vm, out, false);
}

} // namespace gluten

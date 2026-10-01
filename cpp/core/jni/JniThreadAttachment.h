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

#pragma once

#include <jni.h>
#include <utility>

namespace gluten {

// On Linux/glibc, register at the entry of a Gluten-owned native thread before any
// user work. Registration does not attach the thread to a JVM. It must not be
// deferred to the first JNI call, which can occur during pthread cleanup.
// JNI-dependent cleanup must finish before Gluten's final pthread destructor
// pass. This is not an ordering guarantee for arbitrarily re-armed TLS keys.
// On other platforms this is a no-op; attachment behavior remains unchanged.
void initializeNativeThreadJni();

// Existing attachments are borrowed. Only attachments made by Gluten on a
// registered native worker are detached at thread exit. An unmanaged thread's
// creator remains responsible for its JNI lifecycle.
jint getOrAttachCurrentThreadAsDaemon(JavaVM* vm, JNIEnv** out);

// The same ownership/cleanup rules, preserving non-daemon attachment semantics.
jint getOrAttachCurrentThread(JavaVM* vm, JNIEnv** out);

template <typename Function>
auto withJniThreadLifecycle(Function&& function) {
  return [function = std::forward<Function>(function)]() mutable {
    initializeNativeThreadJni();
    function();
  };
}

} // namespace gluten

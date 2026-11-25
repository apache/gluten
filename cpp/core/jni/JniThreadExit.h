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

namespace gluten {

/// Arranges for the calling native thread to be detached from `vm` when the thread exits.
///
/// Call this right after Gluten attaches a native thread to the JVM. A native thread that exits while
/// still attached leaves a JavaThread whose pthread no longer exists; the next walk of the JVM thread
/// list (thread dump, jcmd, JVMTI agents, profilers, EMR log collection) then reads that dead pthread
/// and crashes in pthread_getcpuclockid. Native pool threads do exit while the executor runs, since
/// folly pools retire idle workers after --threadtimeout_ms.
///
/// The detach runs from a pthread TSD destructor and is safe for threads that libhdfs also uses; see
/// JniThreadExit.cc. Calling it more than once on a thread is harmless.
void detachCurrentThreadOnExit(JavaVM* vm);

} // namespace gluten

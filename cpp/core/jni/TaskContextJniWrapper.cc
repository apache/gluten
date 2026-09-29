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
#include "jni/TaskContextJniWrapper.h"

#include "jni/JniCommon.h"

namespace gluten {

int64_t getCurrentSparkTaskAttemptId() {
  JavaVM* vm = getJniCommonState()->getJavaVM();
  JNIEnv* env = nullptr;
  attachCurrentThreadAsDaemonOrThrow(vm, &env);

  static jclass taskContextJniWrapperClass =
      createGlobalClassReferenceOrError(env, "Lorg/apache/gluten/task/TaskContextJniWrapper;");
  static jmethodID currentTaskAttemptIdMethod =
      getStaticMethodIdOrError(env, taskContextJniWrapperClass, "currentTaskAttemptId", "()J");

  jlong id = env->CallStaticLongMethod(taskContextJniWrapperClass, currentTaskAttemptIdMethod);
  checkException(env);
  return static_cast<int64_t>(id);
}

} // namespace gluten

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

#include <exception>
#include <stdexcept>

#include "jni/JniCommon.h"
#include "jni/JniError.h"

extern "C" JNIEXPORT jint JNICALL
Java_org_apache_gluten_jni_JavaExceptionJniTest_roundTrip(JNIEnv* env, jclass, jobject callback, jboolean nested) {
  JNI_METHOD_START
  gluten::getJniErrorState()->ensureInitialized(env);
  checkException(env);
  auto callbackClass = env->GetObjectClass(callback);
  checkException(env);
  auto run = env->GetMethodID(callbackClass, "run", "()V");
  env->DeleteLocalRef(callbackClass);
  checkException(env);
  try {
    env->CallVoidMethod(callback, run);
    checkException(env);
  } catch (...) {
    if (nested) {
      std::throw_with_nested(std::runtime_error("native wrapper"));
    }
    throw;
  }
  return 42;
  JNI_METHOD_END(-1)
}

extern "C" JNIEXPORT void JNICALL
Java_org_apache_gluten_jni_JavaExceptionJniTest_throwNativeException(JNIEnv* env, jclass) {
  JNI_METHOD_START
  gluten::getJniErrorState()->ensureInitialized(env);
  checkException(env);
  throw std::runtime_error("native-only failure");
  JNI_METHOD_END()
}

extern "C" JNIEXPORT void JNICALL Java_org_apache_gluten_jni_JavaExceptionJniTest_close(JNIEnv* env, jclass) {
  JNI_METHOD_START
  gluten::getJniErrorState()->close();
  JNI_METHOD_END()
}

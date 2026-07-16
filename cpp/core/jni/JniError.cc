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
#include "JniError.h"

void gluten::JniErrorState::ensureInitialized(JNIEnv* env) {
  std::lock_guard<std::mutex> lockGuard(mtx_);
  if (initialized_) {
    return;
  }
  initialize(env);
  initialized_ = true;
}

void gluten::JniErrorState::assertInitialized() {
  if (!initialized_) {
    throw gluten::GlutenException("Fatal: JniErrorState::Initialize(...) was not called before using the utility");
  }
}

jclass gluten::JniErrorState::runtimeExceptionClass() {
  assertInitialized();
  return runtimeExceptionClass_;
}

jclass gluten::JniErrorState::illegalAccessExceptionClass() {
  assertInitialized();
  return illegalAccessExceptionClass_;
}

jclass gluten::JniErrorState::glutenExceptionClass() {
  assertInitialized();
  return glutenExceptionClass_;
}

void gluten::JniErrorState::initialize(JNIEnv* env) {
  glutenExceptionClass_ = createGlobalClassReference(env, "Lorg/apache/gluten/exception/GlutenException;");
  ioExceptionClass_ = createGlobalClassReference(env, "Ljava/io/IOException;");
  runtimeExceptionClass_ = createGlobalClassReference(env, "Ljava/lang/RuntimeException;");
  unsupportedOperationExceptionClass_ = createGlobalClassReference(env, "Ljava/lang/UnsupportedOperationException;");
  illegalAccessExceptionClass_ = createGlobalClassReference(env, "Ljava/lang/IllegalAccessException;");
  illegalArgumentExceptionClass_ = createGlobalClassReference(env, "Ljava/lang/IllegalArgumentException;");
  JavaVM* vm;
  if (env->GetJavaVM(&vm) != JNI_OK) {
    throw gluten::GlutenException("Unable to get JavaVM instance");
  }
  vm_ = vm;
}

void gluten::JniErrorState::close() {
  std::lock_guard<std::mutex> lockGuard(mtx_);
  if (closed_) {
    return;
  }
  JNIEnv* env = nullptr;
  attachCurrentThreadAsDaemonOrThrow(vm_, &env);
  env->DeleteGlobalRef(glutenExceptionClass_);
  env->DeleteGlobalRef(ioExceptionClass_);
  env->DeleteGlobalRef(runtimeExceptionClass_);
  env->DeleteGlobalRef(unsupportedOperationExceptionClass_);
  env->DeleteGlobalRef(illegalAccessExceptionClass_);
  env->DeleteGlobalRef(illegalArgumentExceptionClass_);
  closed_ = true;
}

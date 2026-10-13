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
#include <string>
#include <unordered_map>

namespace gluten {

void initVeloxJniUDF(JNIEnv* env);

void finalizeVeloxJniUDF(JNIEnv* env);

void jniRegisterFunctionSignatures(JNIEnv* env);

/// Resolves a registry-declared scalar UDF against the Velox function registry.
/// 'argTypes' is a serialized substrait Type holding a struct of the actual
/// argument types. An argument may be widened in order to bind; an exact match
/// reports no coercions.
///
/// Returns a serialized substrait Type holding a struct of
/// {returnType, coercions}, where 'coercions' is itself a struct with one field
/// per argument: the type to cast that argument to, or NOTHING where it binds
/// as it is. Returns nullptr if no signature binds.
jbyteArray jniResolveUdfType(JNIEnv* env, jstring name, jbyteArray argTypes);

/// Like jniResolveUdfType, for a registry-declared UDAF. The struct is
/// {returnType, intermediateType, coercions}.
jbyteArray jniResolveUdafTypes(JNIEnv* env, jstring name, jbyteArray argTypes);

} // namespace gluten

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

#include "JniUdf.h"
#include "jni/JniCommon.h"
#include "substrait/SubstraitParser.h"
#include "substrait/VeloxToSubstraitType.h"
#include "udf/UdfLoader.h"
#include "utils/Exception.h"

namespace {

static JavaVM* vm;

const std::string kUdfResolverClassPath = "Lorg/apache/spark/sql/expression/UDFResolver$;";

static jclass udfResolverClass;
static jmethodID registerUDFMethod;
static jmethodID registerUDAFMethod;
static jmethodID registerRegistryUDFMethod;
static jmethodID registerRegistryUDAFMethod;

jobject udfResolverInstance(JNIEnv* env) {
  return env->GetStaticObjectField(
      udfResolverClass, env->GetStaticFieldID(udfResolverClass, "MODULE$", kUdfResolverClassPath.c_str()));
}

// Reads a serialized substrait Type holding a struct of argument types.
std::vector<facebook::velox::TypePtr> parseArgTypes(JNIEnv* env, jbyteArray argTypes) {
  const auto safeArray = gluten::getByteArrayElementsSafe(env, argTypes);
  ::substrait::Type parsed;
  if (!parsed.ParseFromArray(safeArray.elems(), safeArray.length())) {
    throw gluten::GlutenException("Failed to parse the argument types of a UDF/UDAF call");
  }
  GLUTEN_CHECK(parsed.has_struct_(), "Expected a struct of argument types");

  std::vector<facebook::velox::TypePtr> types;
  types.reserve(parsed.struct_().types_size());
  for (const auto& type : parsed.struct_().types()) {
    types.push_back(gluten::SubstraitParser::parseType(type));
  }
  return types;
}

jbyteArray serializeType(JNIEnv* env, const ::substrait::Type& type) {
  std::string output;
  type.SerializeToString(&output);
  jbyteArray result = env->NewByteArray(output.length());
  env->SetByteArrayRegion(result, 0, output.length(), reinterpret_cast<const jbyte*>(output.c_str()));
  return result;
}

} // namespace

void gluten::initVeloxJniUDF(JNIEnv* env) {
  if (env->GetJavaVM(&vm) != JNI_OK) {
    throw gluten::GlutenException("Unable to get JavaVM instance");
  }

  // classes
  udfResolverClass = createGlobalClassReferenceOrError(env, kUdfResolverClassPath.c_str());

  // methods
  registerUDFMethod = getMethodIdOrError(env, udfResolverClass, "registerUDF", "(Ljava/lang/String;[B[BZZ)V");
  registerUDAFMethod = getMethodIdOrError(env, udfResolverClass, "registerUDAF", "(Ljava/lang/String;[B[B[BZZ)V");
  registerRegistryUDFMethod = getMethodIdOrError(env, udfResolverClass, "registerRegistryUDF", "(Ljava/lang/String;)V");
  registerRegistryUDAFMethod =
      getMethodIdOrError(env, udfResolverClass, "registerRegistryUDAF", "(Ljava/lang/String;)V");
}

void gluten::finalizeVeloxJniUDF(JNIEnv* env) {
  env->DeleteGlobalRef(udfResolverClass);
}

void gluten::jniRegisterFunctionSignatures(JNIEnv* env) {
  auto udfLoader = gluten::UdfLoader::getInstance();

  const auto& signatures = udfLoader->getRegisteredUdfSignatures();
  for (const auto& signature : signatures) {
    jstring name = env->NewStringUTF(signature->name.c_str());
    jbyteArray returnType = env->NewByteArray(signature->returnType.length());
    env->SetByteArrayRegion(
        returnType, 0, signature->returnType.length(), reinterpret_cast<const jbyte*>(signature->returnType.c_str()));
    jbyteArray argTypes = env->NewByteArray(signature->argTypes.length());
    env->SetByteArrayRegion(
        argTypes, 0, signature->argTypes.length(), reinterpret_cast<const jbyte*>(signature->argTypes.c_str()));
    jobject instance = env->GetStaticObjectField(
        udfResolverClass, env->GetStaticFieldID(udfResolverClass, "MODULE$", kUdfResolverClassPath.c_str()));
    if (!signature->intermediateType.empty()) {
      jbyteArray intermediateType = env->NewByteArray(signature->intermediateType.length());
      env->SetByteArrayRegion(
          intermediateType,
          0,
          signature->intermediateType.length(),
          reinterpret_cast<const jbyte*>(signature->intermediateType.c_str()));
      env->CallVoidMethod(
          instance,
          registerUDAFMethod,
          name,
          returnType,
          argTypes,
          intermediateType,
          signature->variableArity,
          signature->allowTypeConversion);
    } else {
      env->CallVoidMethod(
          instance,
          registerUDFMethod,
          name,
          returnType,
          argTypes,
          signature->variableArity,
          signature->allowTypeConversion);
    }
    checkException(env);
  }

  for (const auto& name : udfLoader->getRegistryUdfNames()) {
    env->CallVoidMethod(udfResolverInstance(env), registerRegistryUDFMethod, env->NewStringUTF(name.c_str()));
    checkException(env);
  }

  for (const auto& name : udfLoader->getRegistryUdafNames()) {
    env->CallVoidMethod(udfResolverInstance(env), registerRegistryUDAFMethod, env->NewStringUTF(name.c_str()));
    checkException(env);
  }
}

jbyteArray gluten::jniResolveUdfType(JNIEnv* env, jstring name, jbyteArray argTypes) {
  const auto returnType = UdfLoader::resolveUdfType(jStringToCString(env, name), parseArgTypes(env, argTypes));
  if (returnType == nullptr) {
    return nullptr;
  }

  google::protobuf::Arena arena;
  VeloxToSubstraitTypeConvertor convertor;
  return serializeType(env, convertor.toSubstraitType(arena, returnType));
}

jbyteArray gluten::jniResolveUdafTypes(JNIEnv* env, jstring name, jbyteArray argTypes) {
  const auto resolved = UdfLoader::resolveUdafTypes(jStringToCString(env, name), parseArgTypes(env, argTypes));
  if (!resolved.has_value()) {
    return nullptr;
  }

  google::protobuf::Arena arena;
  VeloxToSubstraitTypeConvertor convertor;
  // The two types travel as one struct so that the JVM gets them from a single
  // call, and so that it can reuse ConverterUtils to read them back.
  ::substrait::Type out;
  auto* structType = out.mutable_struct_();
  structType->set_nullability(::substrait::Type::NULLABILITY_REQUIRED);
  *structType->add_types() = convertor.toSubstraitType(arena, resolved->first);
  *structType->add_types() = convertor.toSubstraitType(arena, resolved->second);
  return serializeType(env, out);
}

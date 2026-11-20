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

#include "jni/JniFileSystem.h"

#include <gtest/gtest.h>

#include <array>
#include <cstdarg>
#include <cstring>
#include <string_view>

namespace gluten {
namespace {

#ifdef GLUTEN_TEST_BOLT
namespace filesystems = bytedance::bolt::filesystems;
#else
namespace filesystems = facebook::velox::filesystems;
#endif

// Exercise the registered filesystem and its native ReadFile without starting a
// JVM. The fake Java pread copies only the requested bytes into the native
// buffer.
class FakeJvm {
public:
  static FakeJvm &instance() {
    static FakeJvm instance;
    return instance;
  }

  JNIEnv *env() { return &env_; }

  void setInput(std::string_view input) { input_ = input; }

private:
  FakeJvm() {
    vmFunctions_.GetEnv = [](JavaVM *, void **out, jint) -> jint {
      *out = instance().env();
      return JNI_OK;
    };
    functions_.GetJavaVM = [](JNIEnv *, JavaVM **out) -> jint {
      *out = &instance().vm_;
      return JNI_OK;
    };
    functions_.FindClass = [](JNIEnv *, const char *) -> jclass {
      return reinterpret_cast<jclass>(&instance().reference_);
    };
    functions_.NewGlobalRef = [](JNIEnv *, jobject ref) -> jobject {
      return ref;
    };
    functions_.DeleteGlobalRef = [](JNIEnv *, jobject) {};
    functions_.DeleteLocalRef = [](JNIEnv *, jobject) {};
    functions_.GetMethodID = [](JNIEnv *, jclass, const char *name,
                                const char *) -> jmethodID {
      auto &jvm = instance();
      return reinterpret_cast<jmethodID>(std::strcmp(name, "pread") == 0
                                             ? &jvm.preadMethod_
                                             : &jvm.otherMethod_);
    };
    functions_.GetStaticMethodID = functions_.GetMethodID;
    functions_.ExceptionCheck = [](JNIEnv *) -> jboolean { return JNI_FALSE; };
    functions_.NewStringUTF = [](JNIEnv *, const char *) -> jstring {
      return reinterpret_cast<jstring>(&instance().reference_);
    };
    functions_.CallStaticBooleanMethodV = [](JNIEnv *, jclass, jmethodID,
                                             va_list) -> jboolean {
      return JNI_TRUE;
    };
    functions_.CallStaticObjectMethodV = [](JNIEnv *, jclass, jmethodID,
                                            va_list) -> jobject {
      return reinterpret_cast<jobject>(&instance().reference_);
    };
    functions_.CallObjectMethodV = [](JNIEnv *, jobject, jmethodID,
                                      va_list) -> jobject {
      return reinterpret_cast<jobject>(&instance().reference_);
    };
    functions_.CallVoidMethodV = [](JNIEnv *, jobject, jmethodID method,
                                    va_list args) {
      auto &jvm = instance();
      if (method != reinterpret_cast<jmethodID>(&jvm.preadMethod_)) {
        return; // close()
      }
      const auto offset = va_arg(args, jlong);
      const auto length = va_arg(args, jlong);
      auto *buffer = reinterpret_cast<void *>(va_arg(args, jlong));
      ASSERT_GE(offset, 0);
      ASSERT_GE(length, 0);
      ASSERT_LE(offset + length, jvm.input_.size());
      std::memcpy(buffer, jvm.input_.data() + offset, length);
    };
  }

  JNINativeInterface_ functions_{};
  JNIInvokeInterface_ vmFunctions_{};
  JNIEnv env_{&functions_};
  JavaVM vm_{&vmFunctions_};
  char reference_{};
  char preadMethod_{};
  char otherMethod_{};
  std::string_view input_;
};

class JniFileSystemTest : public ::testing::Test {
protected:
  static void SetUpTestSuite() {
#ifdef GLUTEN_TEST_BOLT
    initBoltJniFileSystem(FakeJvm::instance().env());
#else
    initVeloxJniFileSystem(FakeJvm::instance().env());
#endif
    registerJolFileSystem(1);
  }

  static void TearDownTestSuite() {
#ifdef GLUTEN_TEST_BOLT
    finalizeBoltJniFileSystem(FakeJvm::instance().env());
#else
    finalizeVeloxJniFileSystem(FakeJvm::instance().env());
#endif
  }

  std::string_view read(std::string_view input, uint64_t length, char *buffer) {
    FakeJvm::instance().setInput(input);
    auto fs = filesystems::getFileSystem("jol:/binary", nullptr);
    auto file = fs->openFileForRead("jol:/binary", {});
    return file->pread(0, length, buffer);
  }
};

TEST_F(JniFileSystemTest, preservesEmbeddedNulBytes) {
  const std::string_view input("a\0bc", 4);
  std::array<char, 5> buffer{};
  const auto result = read(input, input.size(), buffer.data());
  EXPECT_EQ(result.data(), buffer.data());
  EXPECT_EQ(result, input);
}

TEST_F(JniFileSystemTest, doesNotIncludeBytesBeyondRequestedLength) {
  std::array<char, 6> buffer{'X', 'X', 'X', 'X', 'X', '\0'};
  const auto result = read("abcd", 4, buffer.data());
  EXPECT_EQ(result, "abcd");
  EXPECT_EQ(buffer[4], 'X');
}

TEST_F(JniFileSystemTest, zeroLengthReadReturnsEmptyView) {
  std::array<char, 2> buffer{'X', '\0'};
  const auto result = read("abcd", 0, buffer.data());
  EXPECT_TRUE(result.empty());
  EXPECT_EQ(buffer[0], 'X');
}

} // namespace
} // namespace gluten

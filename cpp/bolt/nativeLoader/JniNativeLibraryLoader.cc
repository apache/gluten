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

#include <dlfcn.h>
#include <jni.h>

#include <atomic>
#include <cerrno>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

#include <glog/logging.h>


// JNI_OnLoad()
typedef int (*OnLoadFunc)(JavaVM*, void*);

// JNI_OnUnload
typedef int (*OnUnLoadFunc)(JavaVM*, void*);

namespace gluten {

/// A JNI libraries loader.
/// User can specify the loading flags: RTLD_GOBAL / RTLD_LOCAL flags
class NativeLibraryLoader {
 public:
  bool init(JNIEnv* env) {
    JavaVM* vm;
    if (env->GetJavaVM(&vm) != JNI_OK) {
      return false;
    }
    vm_ = vm;
    initialized_.store(true, std::memory_order_release);

    return true;
  }

  void loadLibrary(const char* path, int flags) {
    if (!initialized_.load(std::memory_order_acquire)) {
       LOG(ERROR) << dlerror();
      throw std::logic_error("NativeLibraryLoader is not initialized");
    }

    void* handle = dlopen(path, flags);
    if (!handle) {
      LOG(ERROR) << dlerror();
      throw std::logic_error(dlerror());
    }
    LOG(INFO) << "library: (" << path << ") is loaded with flag = " << flags;

    // call JNI_OnLoad
    OnLoadFunc loadFunc = (OnLoadFunc)dlsym(handle, "JNI_OnLoad");
    char* dlerr = dlerror();
    if (dlerr != nullptr || (!loadFunc)) {
        dlclose(handle);
        LOG(ERROR) << dlerr;
        throw std::logic_error("Cannot load symbol: JNI_OnLoad ");
    }
    // if (loadFunc) {
    //   (*loadFunc)(vm_, nullptr);
    // }
    // LOG(INFO) << "JNI_OnLoad is called successfully (" << path << ")";

    // register JNI_UnLoad
    // OnUnLoadFunc unloadFunc = (OnUnLoadFunc)dlsym(handle, "JNI_OnUnLoad");
    // if (unloadFunc) {
    //   unloadHooks_.emplace_back(unloadFunc);
    // }
  }

  void unload() {
    for (auto unload : unloadHooks_) {
      (*unload)(vm_, nullptr);
    }
  }

  static NativeLibraryLoader* getInstance() {
    static NativeLibraryLoader instance;
    return &instance;
  }

 private:
  std::atomic<bool> initialized_{false};
  JavaVM* vm_;

  std::vector<OnUnLoadFunc> unloadHooks_;
};

} // namespace gluten

#ifdef __cplusplus
extern "C" {
#endif

jint JNI_OnLoad(JavaVM* vm, void* reserved) {
  JNIEnv* env;
  if (vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION_1_8) != JNI_OK) {
    return JNI_ERR;
  }

  gluten::NativeLibraryLoader::getInstance()->init(env);

  return JNI_VERSION_1_8;
}

void JNI_OnUnload(JavaVM* vm, void* reserved) {
  auto* loader = gluten::NativeLibraryLoader::getInstance();
  loader->unload();
}

JNIEXPORT jboolean JNICALL Java_org_apache_gluten_jni_BoltJniLibLoader_nativeLoadLibrary( // NOLINT
    JNIEnv* env,
    jclass thisClass,
    jstring path,
    jint rtld_flags) {
  const char* pathPtr = env->GetStringUTFChars(path, NULL);

  auto* loader = gluten::NativeLibraryLoader::getInstance();

  try {
    loader->loadLibrary(pathPtr, rtld_flags);
  } catch (std::exception& ex) {
    std::string err;
    err.append("Failed to load the library: ");
    err.append(std::string(pathPtr));
    err.append("\t");
    err.append(ex.what());
    std::cerr << err;
    env->ThrowNew(thisClass, err.c_str());
  }

  return true;
}

#ifdef __cplusplus
} // extern "C"
#endif

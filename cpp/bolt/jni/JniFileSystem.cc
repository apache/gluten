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

#include "JniFileSystem.h"
#include "bolt/common/io/IoStatistics.h"
#include "jni/JniCommon.h"

namespace {
constexpr std::string_view kJniFsScheme("jni:");
constexpr std::string_view kJolFsScheme("jol:");

JavaVM* vm;

jclass jniFileSystemClass;
jclass jniReadFileClass;
jclass jniWriteFileClass;

jmethodID jniGetFileSystem;
jmethodID jniIsCapableForNewFile;
jmethodID jniFileSystemOpenFileForRead;
jmethodID jniFileSystemOpenFileForWrite;
jmethodID jniFileSystemRemove;
jmethodID jniFileSystemRename;
jmethodID jniFileSystemExists;
jmethodID jniFileSystemList;
jmethodID jniFileSystemMkdir;
jmethodID jniFileSystemRmdir;

jmethodID jniReadFilePread;
jmethodID jniReadFileShouldCoalesce;
jmethodID jniReadFileSize;
jmethodID jniReadFileMemoryUsage;
jmethodID jniReadFileGetNaturalReadSize;
jmethodID jniReadFileClose;

jmethodID jniWriteFileAppend;
jmethodID jniWriteFileFlush;
jmethodID jniWriteFileClose;
jmethodID jniWriteFileSize;

jstring createJString(JNIEnv* env, const std::string_view& path) {
  return env->NewStringUTF(std::string(path).c_str());
}

std::string_view removePathSchema(std::string_view path) {
  unsigned long pos = path.find(':');
  if (pos == std::string::npos) {
    return path;
  }
  return path.substr(pos + 1);
}

class JniReadFile : public bytedance::bolt::ReadFile {
 public:
  explicit JniReadFile(jobject obj) {
    JNIEnv* env = nullptr;
    attachCurrentThreadAsDaemonOrThrow(vm, &env);
    obj_ = env->NewGlobalRef(obj);
    checkException(env);
  }

  ~JniReadFile() override {
    try {
      closeInternal();
      JNIEnv* env = nullptr;
      attachCurrentThreadAsDaemonOrThrow(vm, &env);
      env->DeleteGlobalRef(obj_);
      checkException(env);
    } catch (const std::exception& e) {
      LOG(WARNING) << "Error closing jni read file " << e.what();
    }
  }

  std::string_view pread(uint64_t offset, uint64_t length, void* buf) const override {
    JNIEnv* env = nullptr;
    attachCurrentThreadAsDaemonOrThrow(vm, &env);
    env->CallVoidMethod(
        obj_, jniReadFilePread, static_cast<jlong>(offset), static_cast<jlong>(length), reinterpret_cast<jlong>(buf));
    checkException(env);
    return std::string_view(reinterpret_cast<const char*>(buf));
  }

  bool shouldCoalesce() const override {
    JNIEnv* env = nullptr;
    attachCurrentThreadAsDaemonOrThrow(vm, &env);
    jboolean out = env->CallBooleanMethod(obj_, jniReadFileShouldCoalesce);
    checkException(env);
    return out;
  }

  uint64_t size() const override {
    JNIEnv* env = nullptr;
    attachCurrentThreadAsDaemonOrThrow(vm, &env);
    jlong out = env->CallLongMethod(obj_, jniReadFileSize);
    checkException(env);
    return static_cast<uint64_t>(out);
  }

  uint64_t memoryUsage() const override {
    JNIEnv* env = nullptr;
    attachCurrentThreadAsDaemonOrThrow(vm, &env);
    jlong out = env->CallLongMethod(obj_, jniReadFileMemoryUsage);
    checkException(env);
    return static_cast<uint64_t>(out);
  }

  std::string getName() const override {
    return "<JniReadFile>";
  }

  uint64_t getNaturalReadSize() const override {
    JNIEnv* env = nullptr;
    attachCurrentThreadAsDaemonOrThrow(vm, &env);
    jlong out = env->CallLongMethod(obj_, jniReadFileGetNaturalReadSize);
    checkException(env);
    return static_cast<uint64_t>(out);
  }

 private:
  void closeInternal() {
    JNIEnv* env = nullptr;
    attachCurrentThreadAsDaemonOrThrow(vm, &env);
    env->CallVoidMethod(obj_, jniReadFileClose);
    checkException(env);
  }

  jobject obj_;
};

class JniWriteFile : public bytedance::bolt::WriteFile {
 public:
  explicit JniWriteFile(jobject obj) {
    JNIEnv* env = nullptr;
    attachCurrentThreadAsDaemonOrThrow(vm, &env);
    obj_ = env->NewGlobalRef(obj);
    checkException(env);
  }

  ~JniWriteFile() override {
    try {
      closeInternal();
      JNIEnv* env = nullptr;
      attachCurrentThreadAsDaemonOrThrow(vm, &env);
      env->DeleteGlobalRef(obj_);
      checkException(env);
    } catch (const std::exception& e) {
      LOG(WARNING) << "Error closing jni write file " << e.what();
    }
  }

  void append(std::string_view data) override {
    JNIEnv* env = nullptr;
    attachCurrentThreadAsDaemonOrThrow(vm, &env);
    const void* bytes = data.data();
    unsigned long len = data.size();
    env->CallVoidMethod(obj_, jniWriteFileAppend, static_cast<jlong>(len), reinterpret_cast<jlong>(bytes));
    checkException(env);
  }

  void flush() override {
    JNIEnv* env = nullptr;
    attachCurrentThreadAsDaemonOrThrow(vm, &env);
    env->CallVoidMethod(obj_, jniWriteFileFlush);
    checkException(env);
  }

  void close() override {
    closeInternal();
  }

  uint64_t size() const override {
    JNIEnv* env = nullptr;
    attachCurrentThreadAsDaemonOrThrow(vm, &env);
    jlong out = env->CallLongMethod(obj_, jniWriteFileSize);
    checkException(env);
    return static_cast<uint64_t>(out);
  }

 private:
  void closeInternal() {
    JNIEnv* env = nullptr;
    attachCurrentThreadAsDaemonOrThrow(vm, &env);
    env->CallVoidMethod(obj_, jniWriteFileClose);
    checkException(env);
  }

  jobject obj_;
};

// Convert "xxx:/a/b/c" to "/a/b/c". Probably it's Bolt's job to remove the protocol when calling the member
// functions?
class FileSystemWrapper : public bytedance::bolt::filesystems::FileSystem {
 public:
  static std::shared_ptr<bytedance::bolt::filesystems::FileSystem> wrap(
      std::shared_ptr<bytedance::bolt::filesystems::FileSystem> fs) {
    return std::shared_ptr<bytedance::bolt::filesystems::FileSystem>(new FileSystemWrapper(fs));
  }

  std::string name() const override {
    return fs_->name();
  }

  std::unique_ptr<bytedance::bolt::ReadFile> openFileForRead(
      std::string_view path,
      const bytedance::bolt::filesystems::FileOptions& options) override {
    return fs_->openFileForRead(rewrite(path), options);
  }

  std::unique_ptr<bytedance::bolt::WriteFile> openFileForWrite(
      std::string_view path,
      const bytedance::bolt::filesystems::FileOptions& options) override {
    return fs_->openFileForWrite(rewrite(path), options);
  }

  void remove(std::string_view path) override {
    fs_->remove(rewrite(path));
  }

  void rename(std::string_view oldPath, std::string_view newPath, bool overwrite) override {
    fs_->rename(rewrite(oldPath), rewrite(newPath), overwrite);
  }

  bool exists(std::string_view path) override {
    return fs_->exists(rewrite(path));
  }

  std::vector<std::string> list(std::string_view path) override {
    return fs_->list(rewrite(path));
  }

  void mkdir(std::string_view path) override {
    fs_->mkdir(rewrite(path));
  }

  void rmdir(std::string_view path) override {
    fs_->rmdir(rewrite(path));
  }

 private:
  FileSystemWrapper(std::shared_ptr<bytedance::bolt::filesystems::FileSystem> fs) : FileSystem({}), fs_(fs) {}

  static std::string_view rewrite(std::string_view path) {
    return removePathSchema(path);
  }

  std::shared_ptr<bytedance::bolt::filesystems::FileSystem> fs_;
};

class JniFileSystem : public bytedance::bolt::filesystems::FileSystem {
 public:
  explicit JniFileSystem(jobject obj, std::shared_ptr<const bytedance::bolt::config::ConfigBase> config)
      : FileSystem(config) {
    JNIEnv* env = nullptr;
    attachCurrentThreadAsDaemonOrThrow(vm, &env);
    obj_ = env->NewGlobalRef(obj);
    checkException(env);
  }

  ~JniFileSystem() override {
    try {
      JNIEnv* env = nullptr;
      attachCurrentThreadAsDaemonOrThrow(vm, &env);
      env->DeleteGlobalRef(obj_);
      checkException(env);
    } catch (const std::exception& e) {
      LOG(WARNING) << "Error releasing jni file system " << e.what();
    }
  }

  std::string name() const override {
    return "JNI FS";
  }

  std::unique_ptr<bytedance::bolt::ReadFile> openFileForRead(
      std::string_view path,
      const bytedance::bolt::filesystems::FileOptions& options) override {
    GLUTEN_CHECK(
        options.values.empty(),
        "JniFileSystem::openFileForRead: file options is not empty, this is not currently supported");
    JNIEnv* env = nullptr;
    attachCurrentThreadAsDaemonOrThrow(vm, &env);
    jobject obj = env->CallObjectMethod(obj_, jniFileSystemOpenFileForRead, createJString(env, path));
    checkException(env);
    auto out = std::make_unique<JniReadFile>(obj);
    return out;
  }

  std::unique_ptr<bytedance::bolt::WriteFile> openFileForWrite(
      std::string_view path,
      const bytedance::bolt::filesystems::FileOptions& options) override {
    GLUTEN_CHECK(
        options.values.empty(),
        "JniFileSystem::openFileForWrite: file options is not empty, this is not currently supported");
    JNIEnv* env = nullptr;
    attachCurrentThreadAsDaemonOrThrow(vm, &env);
    jobject obj = env->CallObjectMethod(obj_, jniFileSystemOpenFileForWrite, createJString(env, path));
    checkException(env);
    auto out = std::make_unique<JniWriteFile>(obj);
    return out;
  }

  void remove(std::string_view path) override {
    JNIEnv* env = nullptr;
    attachCurrentThreadAsDaemonOrThrow(vm, &env);
    env->CallVoidMethod(obj_, jniFileSystemRemove, createJString(env, path));
    checkException(env);
  }

  void rename(std::string_view oldPath, std::string_view newPath, bool overwrite) override {
    JNIEnv* env = nullptr;
    attachCurrentThreadAsDaemonOrThrow(vm, &env);
    env->CallVoidMethod(obj_, jniFileSystemRename, createJString(env, oldPath), createJString(env, newPath), overwrite);
    checkException(env);
  }

  bool exists(std::string_view path) override {
    JNIEnv* env = nullptr;
    attachCurrentThreadAsDaemonOrThrow(vm, &env);
    bool out = env->CallBooleanMethod(obj_, jniFileSystemExists, createJString(env, path));
    checkException(env);
    return out;
  }

  std::vector<std::string> list(std::string_view path) override {
    JNIEnv* env = nullptr;
    attachCurrentThreadAsDaemonOrThrow(vm, &env);
    std::vector<std::string> out;
    jobjectArray jarray =
        static_cast<jobjectArray>(env->CallObjectMethod(obj_, jniFileSystemList, createJString(env, path)));
    checkException(env);
    jsize length = env->GetArrayLength(jarray);
    for (jsize i = 0; i < length; ++i) {
      jstring element = static_cast<jstring>(env->GetObjectArrayElement(jarray, i));
      std::string cElement = jStringToCString(env, element);
      out.push_back(cElement);
    }
    return out;
  }

  void mkdir(std::string_view path) override {
    JNIEnv* env = nullptr;
    attachCurrentThreadAsDaemonOrThrow(vm, &env);
    env->CallVoidMethod(obj_, jniFileSystemMkdir, createJString(env, path));
    checkException(env);
  }

  void rmdir(std::string_view path) override {
    JNIEnv* env = nullptr;
    attachCurrentThreadAsDaemonOrThrow(vm, &env);
    env->CallVoidMethod(obj_, jniFileSystemRmdir, createJString(env, path));
    checkException(env);
  }

  static bool isCapableForNewFile(uint64_t size) {
    JNIEnv* env = nullptr;
    attachCurrentThreadAsDaemonOrThrow(vm, &env);
    bool out = env->CallStaticBooleanMethod(jniFileSystemClass, jniIsCapableForNewFile, static_cast<jlong>(size));
    checkException(env);
    return out;
  }

  static std::function<bool(std::string_view)> schemeMatcher() {
    return [](std::string_view filePath) { return filePath.find(kJniFsScheme) == 0; };
  }

  static std::function<
      std::shared_ptr<FileSystem>(std::shared_ptr<const bytedance::bolt::config::ConfigBase>, std::string_view)>
  fileSystemGenerator() {
    return [](std::shared_ptr<const bytedance::bolt::config::ConfigBase> properties, std::string_view filePath) {
      JNIEnv* env = nullptr;
      attachCurrentThreadAsDaemonOrThrow(vm, &env);
      jobject obj = env->CallStaticObjectMethod(jniFileSystemClass, jniGetFileSystem);
      checkException(env);
      // remove "jni:" or "jol:" prefix.
      std::shared_ptr<FileSystem> lfs = FileSystemWrapper::wrap(std::make_shared<JniFileSystem>(obj, properties));
      return lfs;
    };
  }

 private:
  jobject obj_;
};
} // namespace

void gluten::initBoltJniFileSystem(JNIEnv* env) {
  // vm
  if (env->GetJavaVM(&vm) != JNI_OK) {
    throw gluten::GlutenException("Unable to get JavaVM instance");
  }

  // classes
  jniFileSystemClass = createGlobalClassReferenceOrError(env, "Lorg/apache/gluten/fs/JniFilesystem;");
  jniReadFileClass = createGlobalClassReferenceOrError(env, "Lorg/apache/gluten/fs/JniFilesystem$ReadFile;");
  jniWriteFileClass = createGlobalClassReferenceOrError(env, "Lorg/apache/gluten/fs/JniFilesystem$WriteFile;");

  // methods in JniFilesystem
  jniGetFileSystem =
      getStaticMethodIdOrError(env, jniFileSystemClass, "getFileSystem", "()Lorg/apache/gluten/fs/JniFilesystem;");
  jniIsCapableForNewFile = getStaticMethodIdOrError(env, jniFileSystemClass, "isCapableForNewFile", "(J)Z");
  jniFileSystemOpenFileForRead = getMethodIdOrError(
      env, jniFileSystemClass, "openFileForRead", "(Ljava/lang/String;)Lorg/apache/gluten/fs/JniFilesystem$ReadFile;");
  jniFileSystemOpenFileForWrite = getMethodIdOrError(
      env,
      jniFileSystemClass,
      "openFileForWrite",
      "(Ljava/lang/String;)Lorg/apache/gluten/fs/JniFilesystem$WriteFile;");
  jniFileSystemRemove = getMethodIdOrError(env, jniFileSystemClass, "remove", "(Ljava/lang/String;)V");
  jniFileSystemRename =
      getMethodIdOrError(env, jniFileSystemClass, "rename", "(Ljava/lang/String;Ljava/lang/String;Z)V");
  jniFileSystemExists = getMethodIdOrError(env, jniFileSystemClass, "exists", "(Ljava/lang/String;)Z");
  jniFileSystemList = getMethodIdOrError(env, jniFileSystemClass, "list", "(Ljava/lang/String;)[Ljava/lang/String;");
  jniFileSystemMkdir = getMethodIdOrError(env, jniFileSystemClass, "mkdir", "(Ljava/lang/String;)V");
  jniFileSystemRmdir = getMethodIdOrError(env, jniFileSystemClass, "rmdir", "(Ljava/lang/String;)V");

  // methods in JniFilesystem$ReadFile
  jniReadFilePread = getMethodIdOrError(env, jniReadFileClass, "pread", "(JJJ)V");
  jniReadFileShouldCoalesce = getMethodIdOrError(env, jniReadFileClass, "shouldCoalesce", "()Z");
  jniReadFileSize = getMethodIdOrError(env, jniReadFileClass, "size", "()J");
  jniReadFileMemoryUsage = getMethodIdOrError(env, jniReadFileClass, "memoryUsage", "()J");
  jniReadFileGetNaturalReadSize = getMethodIdOrError(env, jniReadFileClass, "getNaturalReadSize", "()J");
  jniReadFileClose = getMethodIdOrError(env, jniReadFileClass, "close", "()V");

  // methods in JniFilesystem$WriteFile
  jniWriteFileAppend = getMethodIdOrError(env, jniWriteFileClass, "append", "(JJ)V");
  jniWriteFileFlush = getMethodIdOrError(env, jniWriteFileClass, "flush", "()V");
  jniWriteFileClose = getMethodIdOrError(env, jniWriteFileClass, "close", "()V");
  jniWriteFileSize = getMethodIdOrError(env, jniWriteFileClass, "size", "()J");
}

void gluten::finalizeBoltJniFileSystem(JNIEnv* env) {
  env->DeleteGlobalRef(jniWriteFileClass);
  env->DeleteGlobalRef(jniReadFileClass);
  env->DeleteGlobalRef(jniFileSystemClass);

  vm = nullptr;
}

// "jol" stands for letting Gluten choose between jni fs and local fs.
// This doesn't implement bytedance::bolt::filesystems::FileSystem since it just
// act as a entry-side router to create JniFilesystem and LocalFilesystem
void gluten::registerJolFileSystem(uint64_t maxFileSize) {
  GLUTEN_CHECK(maxFileSize > 0, "Unexpected max file size for jol fs: " + std::to_string(maxFileSize));

  auto JolSchemeMatcher = [](std::string_view filePath) { return filePath.find(kJolFsScheme) == 0; };

  auto fileSystemGenerator =
      [maxFileSize](
          std::shared_ptr<const bytedance::bolt::config::ConfigBase> properties,
          std::string_view filePath) -> std::shared_ptr<bytedance::bolt::filesystems::FileSystem> {
    // select JNI file if there is enough space
    if (JniFileSystem::isCapableForNewFile(maxFileSize)) {
      return JniFileSystem::fileSystemGenerator()(properties, filePath);
    }

    // otherwise select local file
    // remove "jol:" to make Bolt choose local fs.
    auto localFilePath = removePathSchema(filePath);
    auto fs = FileSystemWrapper::wrap(bytedance::bolt::filesystems::getFileSystem(localFilePath, properties));
    return fs;
  };

  bytedance::bolt::filesystems::registerFileSystem(JolSchemeMatcher, fileSystemGenerator);
}

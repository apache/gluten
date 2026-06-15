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

#include "Runtime.h"
#include "utils/Registry.h"

namespace gluten {
namespace {

Registry<Runtime::Factory>& runtimeFactories() {
  static Registry<Runtime::Factory> registry;
  return registry;
}
Registry<Runtime::Releaser>& runtimeReleasers() {
  static Registry<Runtime::Releaser> registry;
  return registry;
}

} // namespace

void Runtime::registerFactory(const std::string& kind, Runtime::Factory factory, Runtime::Releaser releaser) {
  runtimeFactories().registerObj(kind, std::move(factory));
  runtimeReleasers().registerObj(kind, std::move(releaser));
}

Runtime* Runtime::create(
    const std::string& kind,
    MemoryManager* memoryManager,
    ThreadManager* threadManager,
    const std::unordered_map<std::string, std::string>& sessionConf) {
  auto& factory = runtimeFactories().get(kind);
  return factory(kind, std::move(memoryManager), std::move(threadManager), sessionConf);
}

void Runtime::release(Runtime* runtime) {
  const std::string kind = runtime->kind();
  auto& releaser = runtimeReleasers().get(kind);
  releaser(runtime);
}

std::optional<std::string>* Runtime::localWriteFilesTempPath() {
  // This is thread-local to conform to Java side ColumnarWriteFilesExec's design.
  // FIXME: Pass the path through relevant member functions.
  static thread_local std::optional<std::string> path;
  return &path;
}

std::optional<std::string>* Runtime::localWriteFileName() {
  // This is thread-local to conform to Java side ColumnarWriteFilesExec's design.
  // FIXME: Pass the path through relevant member functions.
  static thread_local std::optional<std::string> fileName;
  return &fileName;
}

} // namespace gluten

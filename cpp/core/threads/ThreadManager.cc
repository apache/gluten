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

#include "ThreadManager.h"

#include "utils/Registry.h"

namespace gluten {
namespace {

Registry<ThreadManager::Factory>& threadManagerFactories() {
  static Registry<ThreadManager::Factory> registry;
  return registry;
}

Registry<ThreadManager::Releaser>& threadManagerReleasers() {
  static Registry<ThreadManager::Releaser> registry;
  return registry;
}

} // namespace

void ThreadManager::registerFactory(const std::string& kind, Factory factory, Releaser releaser) {
  threadManagerFactories().registerObj(kind, std::move(factory));
  threadManagerReleasers().registerObj(kind, std::move(releaser));
}

ThreadManager* ThreadManager::create(const std::string& kind, std::unique_ptr<ThreadInitializer> initializer) {
  auto& factory = threadManagerFactories().get(kind);
  return factory(kind, std::move(initializer));
}

void ThreadManager::release(ThreadManager* threadManager) {
  const std::string kind = threadManager->kind();
  auto& releaser = threadManagerReleasers().get(kind);
  releaser(threadManager);
}

} // namespace gluten

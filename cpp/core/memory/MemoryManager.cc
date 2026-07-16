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
#include "MemoryManager.h"
#include "utils/Registry.h"

namespace gluten {

namespace {
Registry<MemoryManager::Factory>& memoryManagerFactories() {
  static Registry<MemoryManager::Factory> registry;
  return registry;
}
Registry<MemoryManager::Releaser>& memoryManagerReleasers() {
  static Registry<MemoryManager::Releaser> registry;
  return registry;
}
} // namespace

void MemoryManager::registerFactory(const std::string& kind, MemoryManager::Factory factory, Releaser releaser) {
  memoryManagerFactories().registerObj(kind, std::move(factory));
  memoryManagerReleasers().registerObj(kind, std::move(releaser));
}

MemoryManager* MemoryManager::create(const std::string& kind, std::unique_ptr<AllocationListener> listener) {
  auto& factory = memoryManagerFactories().get(kind);
  return factory(kind, std::move(listener));
}

void MemoryManager::release(MemoryManager* memoryManager) {
  const std::string kind = memoryManager->kind();
  auto& releaser = memoryManagerReleasers().get(kind);
  releaser(memoryManager);
}

} // namespace gluten

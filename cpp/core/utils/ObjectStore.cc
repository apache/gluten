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

#include "ObjectStore.h"
#include <glog/logging.h>
#include <iostream>

// static
std::unique_ptr<gluten::ObjectStore> gluten::ObjectStore::create() {
  static std::mutex mtx;
  std::lock_guard<std::mutex> lock(mtx);
  StoreHandle nextId = stores().nextId();
  auto store = std::unique_ptr<gluten::ObjectStore>(new gluten::ObjectStore(nextId));
  StoreHandle storeId = safeCast<StoreHandle>(stores().insert(store.get()));
  GLUTEN_CHECK(storeId == nextId, "Store ID mismatched, this should not happen");
  return store;
}

// static
gluten::ResourceMap<gluten::ObjectStore*>& gluten::ObjectStore::stores() {
  static gluten::ResourceMap<gluten::ObjectStore*> stores;
  return stores;
}

// static
std::pair<gluten::ObjectStore*, gluten::ResourceHandle> gluten::ObjectStore::lookup(gluten::ObjectHandle handle) {
  ResourceHandle storeId = safeCast<ResourceHandle>(handle >> (sizeof(gluten::ResourceHandle) * 8));
  ResourceHandle resourceId = safeCast<ResourceHandle>(handle & std::numeric_limits<ResourceHandle>::max());
  auto store = stores().lookup(storeId);
  return {store, resourceId};
};

gluten::ObjectStore::~ObjectStore() {
  for (;;) {
    std::shared_ptr<void> tempObj;
    {
      const std::lock_guard<std::mutex> lock(mtx_);
      // The empty-check and the pop of the last alive object must be atomic under `mtx_`.
      if (aliveObjects_.empty()) {
        break;
      }
      // destructing in reversed order (the last added object destructed first)
      auto itr = aliveObjects_.rbegin();
      const ResourceHandle handle = (*itr).first;
      const auto& info = (*itr).second;
      const std::string_view typeName = info.typeName;
      const size_t size = info.size;
      VLOG(2) << "Unclosed object ["
              << "Store ID: " << storeId_ << ", Resource handle ID: " << handle << ", TypeName: " << typeName
              << ", Size: " << size
              << "] is found when object store is closing. Gluten will"
                 " destroy it automatically but it's recommended to manually close"
                 " the object through the Java closing API after use,"
                 " to minimize peak memory pressure of the application.";
      tempObj = store_.lookup(handle);
      store_.erase(handle);
      aliveObjects_.erase(handle);
    }
    tempObj.reset(); // this will call the destructor of the object
  }
  stores().erase(storeId_);
}

void gluten::ObjectStore::releaseInternal(gluten::ResourceHandle handle) {
  const std::lock_guard<std::mutex> lock(mtx_);
  store_.erase(handle);
  aliveObjects_.erase(handle);
}

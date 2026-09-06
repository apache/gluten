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

#include "memory/SparkMmapAllocator.h"

#include <algorithm>

#include "velox/common/base/Exceptions.h"
#include "velox/common/memory/Memory.h"

namespace gluten {

using facebook::velox::memory::AllocationTraits;

size_t SparkMmapAllocator::allocatedBytes() const {
  return AllocationTraits::pageBytes(numAllocated());
}

size_t SparkMmapAllocator::capacity() const {
  return std::max(governedCapacity(), allocatedBytes());
}

size_t SparkMmapAllocator::setCapacity(size_t from, size_t to) {
  VELOX_CHECK_LE(to, staticCapacityBytes_, "Cache capacity would exceed the configured size");

  // Clamped below by what is allocated, so that heavy pinning cannot cost the caller its ability
  // to adjust: a target eviction could not reach would otherwise be stored and never seen, since
  // what comes back is held up at the allocated bytes.
  //
  // Clamped above by the configured size. Defensive; nothing should be asking for more.
  //
  // Rounded down to cancel the rounding up on the reading side: headroom is measured as
  // numPages(capacity()) - numAllocated(), and numPages() rounds up, so an unaligned bound of
  // 5000 bytes would be read as two whole pages -- 8192 -- and admit almost a page more than was
  // reserved.
  const size_t target =
      std::min(std::max(pageAlignedDown(to), allocatedBytes()), pageAlignedDown(staticCapacityBytes_));

  // Rare: an allocation was in flight when the bound last moved, and a whole check interval has
  // not been enough to evict it back down. Reported rather than refused; see the header for why.
  if (from < capacity()) {
    VELOX_MEM_LOG_EVERY_MS(WARNING, 1000) << "Cache holds " << capacity() << " bytes against " << from
                                          << " reserved for it; the difference is reported back rather than refused";
  }

  governedCapacityBytes_.store(target, std::memory_order_relaxed);
  return capacity();
}

} // namespace gluten

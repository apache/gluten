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

#include <IO/ReadBuffer.h>
#include <Interpreters/Context_fwd.h>
#include <base/types.h>
#include <boost/core/noncopyable.hpp>
#include <roaring/roaring.hh>

namespace local_engine
{

static constexpr auto DV_FILE_FORMAT_VERSION_ID_V1 = std::byte{1};

/**
  * Roaring bitmap data.
  * For a description of the roaring_bitmap_t, see: https://github.com/RoaringBitmap/CRoaring
  */
class DeltaDVRoaringBitmapArray
{
    static constexpr Int64 MAX_REPRESENTABLE_VALUE
        = (static_cast<UInt64>(INT32_MAX - 1) << 32) | (static_cast<UInt64>(INT32_MIN) & 0xFFFFFFFFL);
    std::vector<roaring::Roaring> roaring_bitmap_array;

    static std::pair<UInt32, UInt32> decompose_high_low_bytes(UInt64 value);
    static UInt64 compose_from_high_low_bytes(UInt32 high, UInt32 low);
    void rb_extend_bitmaps(Int32 new_length);
    void rb_shrink_bitmaps(Int32 new_length);

public:
    explicit DeltaDVRoaringBitmapArray();
    ~DeltaDVRoaringBitmapArray() = default;
    bool operator==(const DeltaDVRoaringBitmapArray & other) const;
    UInt64 cardinality() const;
    void rb_read(const String & file_path, Int32 offset, Int32 data_size, DB::ContextPtr context);
    bool rb_contains(Int64 x) const;
    bool rb_is_empty() const;
    void rb_clear();
    void rb_add(Int64 value);
    void rb_merge(const DeltaDVRoaringBitmapArray & that);
    void merge(const String & that);
    void rb_or(const DeltaDVRoaringBitmapArray & that);
    String serialize() const;
    void deserialize(DB::ReadBuffer & buf);
    std::optional<Int64> last();
};

}
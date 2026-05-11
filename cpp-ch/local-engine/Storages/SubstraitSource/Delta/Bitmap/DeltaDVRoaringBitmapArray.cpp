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
#include "DeltaDVRoaringBitmapArray.h"

#include <zlib.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Storages/SubstraitSource/ReadBufferBuilder.h>
#include <substrait/plan.pb.h>
#include <roaring/roaring.hh>
#include <Poco/URI.h>
#include <Common/PODArray.h>

namespace DB
{
namespace ErrorCodes
{
extern const int TOO_LARGE_ARRAY_SIZE;
extern const int INCORRECT_DATA;
extern const int BAD_ARGUMENTS;
}
}

namespace local_engine
{
using namespace DB;

std::pair<UInt32, UInt32> DeltaDVRoaringBitmapArray::decompose_high_low_bytes(UInt64 value)
{
    return {static_cast<UInt32>(value >> 32), static_cast<UInt32>(value & 0xFFFFFFFF)};
}

UInt64 DeltaDVRoaringBitmapArray::compose_from_high_low_bytes(UInt32 high, UInt32 low)
{
    return (static_cast<uint64_t>(high) << 32) | low;
}

DeltaDVRoaringBitmapArray::DeltaDVRoaringBitmapArray()
{
}

void DeltaDVRoaringBitmapArray::rb_read(const String & file_path, Int32 offset, Int32 data_size, DB::ContextPtr context)
{
    // maybe encode in scala
    std::string decoded;
    Poco::URI::decode(file_path, decoded);
    std::string encoded;
    Poco::URI::encode(decoded, "", encoded);

    substrait::ReadRel::LocalFiles::FileOrFiles file_info;
    file_info.set_uri_file(encoded);
    file_info.set_start(offset);
    file_info.set_length(data_size);

    const Poco::URI file_uri(encoded);
    ReadBufferBuilderPtr read_buffer_builder = ReadBufferBuilderFactory::instance().createBuilder(file_uri.getScheme(), context);
    auto * in = dynamic_cast<DB::SeekableReadBuffer *>(read_buffer_builder->build(file_info).release());
    if (in == nullptr)
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Failed to create a valid SeekableReadBuffer.");

    in->seek(offset, SEEK_SET);

    int size;
    readBinaryBigEndian(size, *in);

    if (data_size != size)
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "The size of the deletion vector is mismatch.");

    int checksum_value = static_cast<Int32>(crc32_z(0L, reinterpret_cast<unsigned char *>(in->position()), size));
    deserialize(*in);

    int expected_checksum;
    readBinaryBigEndian(expected_checksum, *in);
    if (expected_checksum != checksum_value)
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Checksum mismatch.");
}

UInt64 DeltaDVRoaringBitmapArray::cardinality() const
{
    UInt64 sum = 0;
    for (const auto & r : roaring_bitmap_array)
        sum += r.cardinality();
    return sum;
}

bool DeltaDVRoaringBitmapArray::rb_contains(Int64 x) const
{
    auto [high, low] = decompose_high_low_bytes(x);
    if (high >= roaring_bitmap_array.size())
        return false;

    return roaring_bitmap_array[high].contains(low);
}

void DeltaDVRoaringBitmapArray::rb_clear()
{
    std::vector<roaring::Roaring>().swap(roaring_bitmap_array);
}

bool DeltaDVRoaringBitmapArray::rb_is_empty() const
{
    return std::ranges::all_of(roaring_bitmap_array.begin(), roaring_bitmap_array.end(), [](const auto & rb) { return rb.isEmpty(); });
}

void DeltaDVRoaringBitmapArray::rb_add(Int64 x)
{
    const UInt64 value = static_cast<UInt64>(x);
    assert(value >= 0 && value <= MAX_REPRESENTABLE_VALUE);
    auto [high, low] = decompose_high_low_bytes(value);
    if (high >= roaring_bitmap_array.size())
        rb_extend_bitmaps(high + 1);
    roaring_bitmap_array[high].add(low);
}

void DeltaDVRoaringBitmapArray::rb_extend_bitmaps(Int32 new_length)
{
    if (roaring_bitmap_array.size() >= new_length)
        return;
    roaring_bitmap_array.resize(new_length);
}

void DeltaDVRoaringBitmapArray::rb_shrink_bitmaps(Int32 new_length)
{
    if (roaring_bitmap_array.size() <= new_length)
        return;
    roaring_bitmap_array.resize(new_length);
}

void DeltaDVRoaringBitmapArray::rb_merge(const DeltaDVRoaringBitmapArray & that)
{
    rb_or(that);
}

void DeltaDVRoaringBitmapArray::merge(const String & that)
{
    DB::ReadBufferFromString rb(that);
    DeltaDVRoaringBitmapArray that_bitmap;
    that_bitmap.deserialize(rb);
    rb_merge(that_bitmap);
}

void DeltaDVRoaringBitmapArray::rb_or(const DeltaDVRoaringBitmapArray & that)
{
    if (roaring_bitmap_array.size() < that.roaring_bitmap_array.size())
        rb_extend_bitmaps(that.roaring_bitmap_array.size());
    const Int32 count = that.roaring_bitmap_array.size();
    for (Int32 i = 0; i < count; ++i)
        roaring_bitmap_array[i] |= that.roaring_bitmap_array[i];
}
bool DeltaDVRoaringBitmapArray::operator==(const DeltaDVRoaringBitmapArray & other) const
{
    if (this == &other)
        return true;

    return roaring_bitmap_array == other.roaring_bitmap_array;
}


String DeltaDVRoaringBitmapArray::serialize() const
{
    DB::WriteBufferFromOwnString out;
    constexpr Int32 magic_number = 1681511377;
    writeBinaryLittleEndian(magic_number, out);
    Int64 size = roaring_bitmap_array.size();
    writeBinaryLittleEndian(size, out);

    for (Int32 i = 0; i < roaring_bitmap_array.size(); ++i)
    {
        writeBinaryLittleEndian(i, out);
        std::unique_ptr<roaring::Roaring> bitmap = std::make_unique<roaring::Roaring>(roaring_bitmap_array.at(i));
        bitmap->runOptimize();
        auto size_in_bytes = bitmap->getSizeInBytes();
        std::unique_ptr<char[]> buf(new char[size_in_bytes]);
        bitmap->write(buf.get());
        out.write(buf.get(), size_in_bytes);
    }

    return out.str();
}

void DeltaDVRoaringBitmapArray::deserialize(DB::ReadBuffer & buf)
{
    Int32 magic_num;
    readBinaryLittleEndian(magic_num, buf);
    if (magic_num != 1681511377)
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "The magic num is mismatch.");

    int64_t bitmap_array_size;
    readBinaryLittleEndian(bitmap_array_size, buf);

    roaring_bitmap_array.reserve(bitmap_array_size);
    for (size_t i = 0; i < bitmap_array_size; ++i)
    {
        int bitmap_index;
        readBinaryLittleEndian(bitmap_index, buf);
        roaring::Roaring r = roaring::Roaring::read(buf.position());
        size_t current_bitmap_size = r.getSizeInBytes();
        buf.ignore(current_bitmap_size);
        roaring_bitmap_array.push_back(r);
    }
}

std::optional<Int64> DeltaDVRoaringBitmapArray::last()
{
    if (roaring_bitmap_array.empty() || roaring_bitmap_array.back().isEmpty())
        return std::nullopt;

    return compose_from_high_low_bytes(roaring_bitmap_array.size(), roaring_bitmap_array.back().maximum());
}


}
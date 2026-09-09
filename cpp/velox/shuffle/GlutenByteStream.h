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

// TODO: wait to delete after rss sort reader refactored.
#include "velox/common/memory/ByteStream.h"

namespace facebook::velox {

class GlutenByteInputStream : public ByteInputStream {
 protected:
  /// TODO Remove after refactoring SpillInput.
  GlutenByteInputStream() {}

 public:
  explicit GlutenByteInputStream(std::vector<ByteRange> ranges) {
    ranges_ = std::move(ranges);
    VELOX_CHECK(!ranges_.empty());
    current_ = &ranges_[0];
  }

  /// Disable copy constructor.
  GlutenByteInputStream(const GlutenByteInputStream&) = delete;

  /// Disable copy assignment operator.
  GlutenByteInputStream& operator=(const GlutenByteInputStream& other) = delete;

  /// Enable move constructor.
  GlutenByteInputStream(GlutenByteInputStream&& other) noexcept = delete;

  /// Enable move assignment operator.
  GlutenByteInputStream& operator=(GlutenByteInputStream&& other) noexcept {
    if (this != &other) {
      ranges_ = std::move(other.ranges_);
      current_ = other.current_;
      other.current_ = nullptr;
    }
    return *this;
  }

  /// TODO Remove after refactoring SpillInput.
  virtual ~GlutenByteInputStream() = default;

  std::vector<ByteRange> ranges_;

  /// Returns total number of bytes available in the stream.
  size_t size() const {
    size_t total = 0;
    for (const auto& range : ranges_) {
      total += range.size;
    }
    return total;
  }

  /// Returns true if all input has been read.
  ///
  /// TODO: Remove 'virtual' after refactoring SpillInput.
  virtual bool atEnd() const {
    if (!current_) {
      return false;
    }
    if (current_->position < current_->size) {
      return false;
    }

    VELOX_CHECK(current_ >= ranges_.data() && current_ <= &ranges_.back());
    return current_ == &ranges_.back();
  }

  /// Returns current position (number of bytes from the start) in the stream.
  std::streampos tellp() const {
    if (ranges_.empty()) {
      return 0;
    }
    VELOX_DCHECK_NOT_NULL(current_);
    int64_t size = 0;
    for (auto& range : ranges_) {
      if (&range == current_) {
        return current_->position + size;
      }
      size += range.size;
    }
    VELOX_FAIL("GlutenByteInputStream 'current_' is not in 'ranges_'.");
  }

  /// Moves current position to specified one.
  void seekp(std::streampos position) {
    if (ranges_.empty() && position == 0) {
      return;
    }
    int64_t toSkip = position;
    for (auto& range : ranges_) {
      if (toSkip <= range.size) {
        current_ = &range;
        current_->position = toSkip;
        return;
      }
      toSkip -= range.size;
    }
    static_assert(sizeof(std::streamsize) <= sizeof(long long));
    VELOX_FAIL("Seeking past end of GlutenByteInputStream: {}", static_cast<long long>(position));
  }

  /// Returns the remaining size left from current reading position.
  size_t remainingSize() const {
    if (ranges_.empty()) {
      return 0;
    }
    const auto* lastRange = &ranges_[ranges_.size() - 1];
    auto cur = current_;
    size_t total = cur->size - cur->position;
    while (++cur <= lastRange) {
      total += cur->size;
    }
    return total;
  }

  std::string toString() const {
    std::stringstream oss;
    oss << ranges_.size() << " ranges (position/size) [";
    for (const auto& range : ranges_) {
      oss << "(" << range.position << "/" << range.size << (&range == current_ ? " current" : "") << ")";
      if (&range != &ranges_.back()) {
        oss << ",";
      }
    }
    oss << "]";
    return oss.str();
  }

  uint8_t readByte() {
    if (current_->position < current_->size) {
      return current_->buffer[current_->position++];
    }
    next();
    return readByte();
  }

  void readBytes(uint8_t* bytes, int32_t size) {
    VELOX_CHECK_GE(size, 0, "Attempting to read negative number of bytes");
    int32_t offset = 0;
    for (;;) {
      int32_t available = current_->size - current_->position;
      int32_t numUsed = std::min(available, size);
      simd::memcpy(bytes + offset, current_->buffer + current_->position, numUsed);
      offset += numUsed;
      size -= numUsed;
      current_->position += numUsed;
      if (!size) {
        return;
      }
      next();
    }
  }

  template <typename T>
  T read() {
    if (current_->position + sizeof(T) <= current_->size) {
      current_->position += sizeof(T);
      return *reinterpret_cast<const T*>(current_->buffer + current_->position - sizeof(T));
    }
    // The number straddles two buffers. We read byte by byte and make
    // a little-endian uint64_t. The bytes can be cast to any integer
    // or floating point type since the wire format has the machine byte order.
    static_assert(sizeof(T) <= sizeof(uint64_t));
    uint64_t value = 0;
    for (int32_t i = 0; i < sizeof(T); ++i) {
      value |= static_cast<uint64_t>(readByte()) << (i * 8);
    }
    return *reinterpret_cast<const T*>(&value);
  }

  template <typename Char>
  void readBytes(Char* data, int32_t size) {
    readBytes(reinterpret_cast<uint8_t*>(data), size);
  }

  /// Returns a view over the read buffer for up to 'size' next
  /// bytes. The size of the value may be less if the current byte
  /// range ends within 'size' bytes from the current position.  The
  /// size will be 0 if at end.
  virtual std::string_view nextView(int64_t size) {
    VELOX_CHECK_GE(size, 0, "Attempting to view negative number of bytes");
    if (current_->position == current_->size) {
      if (current_ == &ranges_.back()) {
        return std::string_view(nullptr, 0);
      }
      next();
    }
    VELOX_CHECK(current_->size);
    auto position = current_->position;
    auto viewSize = std::min(current_->size - current_->position, size);
    current_->position += viewSize;
    return std::string_view(reinterpret_cast<char*>(current_->buffer) + position, viewSize);
  }

  void skip(int32_t size) {
    VELOX_CHECK_GE(size, 0, "Attempting to skip negative number of bytes");
    for (;;) {
      int32_t available = current_->size - current_->position;
      int32_t numUsed = std::min(available, size);
      size -= numUsed;
      current_->position += numUsed;
      if (!size) {
        return;
      }
      next();
    }
  }

 protected:
  /// Sets 'current_' to point to the next range of input.  // The
  /// input is consecutive ByteRanges in 'ranges_' for the base class
  /// but any view over external buffers can be made by specialization.
  ///
  /// TODO: Remove 'virtual' after refactoring SpillInput.
  virtual void next(bool throwIfPastEnd = true) {
    VELOX_CHECK(current_ >= &ranges_[0]);
    size_t position = current_ - &ranges_[0];
    VELOX_CHECK_LT(position, ranges_.size());
    if (position == ranges_.size() - 1) {
      if (throwIfPastEnd) {
        VELOX_FAIL("Reading past end of GlutenByteInputStream");
      }
      return;
    }
    ++current_;
    current_->position = 0;
  }

  // TODO: Remove  after refactoring SpillInput.
  const std::vector<ByteRange>& ranges() const {
    return ranges_;
  }

  // TODO: Remove  after refactoring SpillInput.
  void setRange(ByteRange range) {
    ranges_.resize(1);
    ranges_[0] = range;
    current_ = ranges_.data();
  }
};

template <>
inline Timestamp GlutenByteInputStream::read<Timestamp>() {
  Timestamp value;
  readBytes(reinterpret_cast<uint8_t*>(&value), sizeof(value));
  return value;
}

template <>
inline int128_t GlutenByteInputStream::read<int128_t>() {
  int128_t value;
  readBytes(reinterpret_cast<uint8_t*>(&value), sizeof(value));
  return value;
}

} // namespace facebook::velox

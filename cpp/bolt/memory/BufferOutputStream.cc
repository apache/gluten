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

#include "memory/BufferOutputStream.h"

namespace gluten {
BufferOutputStream::BufferOutputStream(
    bytedance::bolt::memory::MemoryPool* pool,
    int32_t initialSize,
    bytedance::bolt::OutputStreamListener* listener)
    : bytedance::bolt::OutputStream(listener) {
  buffer_ = bytedance::bolt::AlignedBuffer::allocate<char>(initialSize, pool);
  buffer_->setSize(0);
}

void BufferOutputStream::write(const char* s, std::streamsize count) {
  bytedance::bolt::AlignedBuffer::appendTo(&buffer_, s, count);
}

std::streampos BufferOutputStream::tellp() const {
  return buffer_->size();
}

void BufferOutputStream::seekp(std::streampos pos) {
  buffer_->setSize(pos);
}

bytedance::bolt::BufferPtr BufferOutputStream::getBuffer() const {
  return buffer_;
}
} // namespace gluten

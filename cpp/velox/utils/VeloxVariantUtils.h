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

#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"

namespace gluten {

/// Spark's UnsafeRow and UnsafeArrayData store a Variant like a binary value
/// whose bytes are [int32 size of value][value][metadata], see Spark's
/// UnsafeWriter.write(int, VariantVal). Velox's VARIANT is
/// ROW(metadata VARBINARY, value VARBINARY), which UnsafeRowFast would write
/// as a nested struct. The helpers below convert VARIANT values to and from
/// that binary form, so that UnsafeRowFast can read and write them as
/// VARBINARY.

/// Returns true if 'type' is or contains VARIANT.
bool containsVariantType(const facebook::velox::TypePtr& type);

/// Returns 'type' with every VARIANT replaced by VARBINARY.
facebook::velox::TypePtr variantToUnsafeRowType(const facebook::velox::TypePtr& type);

/// Converts the VARIANT values in 'vector' to their UnsafeRow binary form. The
/// result has type variantToUnsafeRowType(vector->type()).
facebook::velox::VectorPtr variantToUnsafeRowBinary(
    facebook::velox::VectorPtr vector,
    facebook::velox::memory::MemoryPool* pool);

/// Inverse of variantToUnsafeRowBinary. 'vector' has type
/// variantToUnsafeRowType('type'). The result has type 'type'.
facebook::velox::VectorPtr unsafeRowBinaryToVariant(
    facebook::velox::VectorPtr vector,
    const facebook::velox::TypePtr& type,
    facebook::velox::memory::MemoryPool* pool);

} // namespace gluten

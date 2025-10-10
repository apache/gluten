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

#include "VariantToVectorConverter.h"

#include "bolt/vector/FlatVector.h"

namespace gluten {

namespace {
template <TypeKind KIND>
VectorPtr setVectorFromVariantsByKind(const std::vector<variant>& values, const TypePtr& type, memory::MemoryPool* pool) {
  using T = typename TypeTraits<KIND>::NativeType;

  auto flatVector = BaseVector::create<FlatVector<T>>(type, values.size(), pool);

  for (vector_size_t i = 0; i < values.size(); i++) {
    if (values[i].isNull()) {
      flatVector->setNull(i, true);
    } else {
      flatVector->set(i, values[i].value<T>());
    }
  }
  return flatVector;
}

template <>
VectorPtr setVectorFromVariantsByKind<TypeKind::VARBINARY>(
    const std::vector<variant>& /* values */,
    const TypePtr& /* type */,
    memory::MemoryPool* /* pool */) {
  BOLT_UNSUPPORTED("Return of VARBINARY data is not supported");
}

template <>
VectorPtr setVectorFromVariantsByKind<TypeKind::VARCHAR>(
    const std::vector<variant>& values,
    const TypePtr& type,
    memory::MemoryPool* pool) {
  auto flatVector = BaseVector::create<FlatVector<StringView>>(type, values.size(), pool);

  for (vector_size_t i = 0; i < values.size(); i++) {
    if (values[i].isNull()) {
      flatVector->setNull(i, true);
    } else {
      flatVector->set(i, StringView(values[i].value<TypeKind::VARCHAR>()));
    }
  }
  return flatVector;
}
} // namespace

VectorPtr setVectorFromVariants(const TypePtr& type, const std::vector<variant>& values, memory::MemoryPool* pool) {
  return BOLT_DYNAMIC_SCALAR_TYPE_DISPATCH(setVectorFromVariantsByKind, type->kind(), values, type, pool);
}

} // namespace gluten

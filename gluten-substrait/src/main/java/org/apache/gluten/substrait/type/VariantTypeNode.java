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
package org.apache.gluten.substrait.type;

import io.substrait.proto.Type;

/**
 * Spark's VariantType. Substrait has no Variant type, so it is sent as struct<metadata: binary,
 * value: binary>, the storage layout of Velox's VARIANT, marked with {@link #TYPE_VARIATION} so
 * that the native side can tell it apart from a plain struct.
 */
public class VariantTypeNode extends TypeNode {
  /** Keep in sync with kVariantTypeVariation in cpp/velox/substrait/SubstraitParser.h. */
  public static final int TYPE_VARIATION = 1;

  public VariantTypeNode(Boolean nullable) {
    super(nullable);
  }

  @Override
  public Type toProtobuf() {
    Type binary =
        Type.newBuilder()
            .setBinary(
                Type.Binary.newBuilder().setNullability(Type.Nullability.NULLABILITY_REQUIRED))
            .build();
    Type.Struct.Builder structBuilder =
        Type.Struct.newBuilder()
            .setTypeVariationReference(TYPE_VARIATION)
            .setNullability(
                nullable
                    ? Type.Nullability.NULLABILITY_NULLABLE
                    : Type.Nullability.NULLABILITY_REQUIRED)
            .addTypes(binary)
            .addTypes(binary)
            .addNames("metadata")
            .addNames("value");
    return Type.newBuilder().setStruct(structBuilder.build()).build();
  }
}

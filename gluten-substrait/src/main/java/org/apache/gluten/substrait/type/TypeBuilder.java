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

import java.util.List;

public class TypeBuilder {
  private TypeBuilder() {}

  public static TypeNode makeFP32(Boolean nullable) {
    return new FP32TypeNode(nullable);
  }

  public static TypeNode makeFP64(Boolean nullable) {
    return new FP64TypeNode(nullable);
  }

  public static TypeNode makeBoolean(Boolean nullable) {
    return new BooleanTypeNode(nullable);
  }

  public static TypeNode makeString(Boolean nullable) {
    return new StringTypeNode(nullable);
  }

  public static TypeNode makeFixedChar(Boolean nullable, int length) {
    return new FixedCharTypeNode(nullable, length);
  }

  public static TypeNode makeFixedBinary(Boolean nullable, int length) {
    return new FixedBinaryTypeNode(nullable, length);
  }

  public static TypeNode makeBinary(Boolean nullable) {
    return new BinaryTypeNode(nullable);
  }

  public static TypeNode makeI8(Boolean nullable) {
    return new I8TypeNode(nullable);
  }

  public static TypeNode makeI16(Boolean nullable) {
    return new I16TypeNode(nullable);
  }

  public static TypeNode makeI32(Boolean nullable) {
    return new I32TypeNode(nullable);
  }

  public static TypeNode makeI64(Boolean nullable) {
    return new I64TypeNode(nullable);
  }

  public static TypeNode makeDate(Boolean nullable) {
    return new DateTypeNode(nullable);
  }

  public static TypeNode makeIntervalYear(Boolean nullable) {
    return new IntervalYearTypeNode(nullable);
  }

  public static TypeNode makeDecimal(Boolean nullable, Integer precision, Integer scale) {
    return new DecimalTypeNode(nullable, precision, scale);
  }

  public static TypeNode makeTimestamp(Boolean nullable) {
    return new TimestampTypeNode(nullable);
  }

  public static TypeNode makeTimestampNTZ(Boolean nullable) {
    return new TimestampNTZTypeNode(nullable);
  }

  public static TypeNode makeStruct(Boolean nullable, List<TypeNode> types, List<String> names) {
    return new StructNode(nullable, types, names);
  }

  public static TypeNode makeStruct(Boolean nullable, List<TypeNode> types) {
    return new StructNode(nullable, types);
  }

  public static TypeNode makeMap(Boolean nullable, TypeNode keyType, TypeNode valType) {
    return new MapNode(nullable, keyType, valType);
  }

  public static TypeNode makeList(Boolean nullable, TypeNode nestedType) {
    return new ListNode(nullable, nestedType);
  }

  public static TypeNode makeNothing() {
    return new NothingNode();
  }
}

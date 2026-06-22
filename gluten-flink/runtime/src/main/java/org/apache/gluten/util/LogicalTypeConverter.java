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
package org.apache.gluten.util;

import io.github.zhztheplayer.velox4j.type.Type;

import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DayTimeIntervalType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SymbolType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarCharType;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

// Convertor to convert Flink LogicalType to velox data Type
public class LogicalTypeConverter {
  private interface VLTypeConverter {
    Type build(LogicalType logicalType);
  }

  // Exact class matches
  private static Map<Class<?>, VLTypeConverter> converters =
      Map.ofEntries(
          Map.entry(
              BooleanType.class,
              logicalType -> new io.github.zhztheplayer.velox4j.type.BooleanType()),
          Map.entry(
              IntType.class, logicalType -> new io.github.zhztheplayer.velox4j.type.IntegerType()),
          Map.entry(
              BigIntType.class,
              logicalType -> new io.github.zhztheplayer.velox4j.type.BigIntType()),
          Map.entry(
              DoubleType.class,
              logicalType -> new io.github.zhztheplayer.velox4j.type.DoubleType()),
          Map.entry(
              VarCharType.class,
              logicalType -> new io.github.zhztheplayer.velox4j.type.VarCharType()),
          Map.entry(
              CharType.class, logicalType -> new io.github.zhztheplayer.velox4j.type.VarCharType()),
          Map.entry(
              TimestampType.class,
              logicalType -> {
                TimestampType timestampType = (TimestampType) logicalType;
                return new io.github.zhztheplayer.velox4j.type.TimestampType(
                    timestampType.getPrecision(), false);
              }),
          Map.entry(
              DecimalType.class,
              logicalType -> {
                DecimalType decimalType = (DecimalType) logicalType;
                return new io.github.zhztheplayer.velox4j.type.DecimalType(
                    decimalType.getPrecision(), decimalType.getScale());
              }),
          Map.entry(
              DayTimeIntervalType.class,
              logicalType -> new io.github.zhztheplayer.velox4j.type.BigIntType()),
          Map.entry(
              DateType.class, logicalType -> new io.github.zhztheplayer.velox4j.type.DateType()),
          Map.entry(
              RowType.class,
              logicalType -> {
                RowType flinkRowType = (RowType) logicalType;
                List<Type> fieldTypes =
                    flinkRowType.getChildren().stream()
                        .map(LogicalTypeConverter::toVLType)
                        .collect(Collectors.toList());
                return new io.github.zhztheplayer.velox4j.type.RowType(
                    flinkRowType.getFieldNames(), fieldTypes);
              }),
          Map.entry(
              ArrayType.class,
              logicalType -> {
                ArrayType arrayType = (ArrayType) logicalType;
                Type elementType = toVLType(arrayType.getElementType());
                return io.github.zhztheplayer.velox4j.type.ArrayType.create(elementType);
              }),
          Map.entry(
              MapType.class,
              logicalType -> {
                MapType mapType = (MapType) logicalType;
                Type keyType = toVLType(mapType.getKeyType());
                Type valueType = toVLType(mapType.getValueType());
                return io.github.zhztheplayer.velox4j.type.MapType.create(keyType, valueType);
              }),
          // Map the flink's `TimestampLTZ` type to velox `Timestamp` type. And the timezone would
          // be specified by using flink's table config `LOCAL_TIME_ZONE`, which would be passed to
          // velox's `session_timezone` config.
          Map.entry(
              LocalZonedTimestampType.class,
              logicalType -> {
                LocalZonedTimestampType timestampType = (LocalZonedTimestampType) logicalType;
                return new io.github.zhztheplayer.velox4j.type.TimestampType(
                    timestampType.getPrecision(), true);
              }),
          Map.entry(
              TinyIntType.class,
              logicalType -> new io.github.zhztheplayer.velox4j.type.TinyIntType()),
          Map.entry(
              SymbolType.class,
              logicalType -> new io.github.zhztheplayer.velox4j.type.VarCharType()));

  public static Type toVLType(LogicalType logicalType) {
    VLTypeConverter converter = converters.get(logicalType.getClass());
    if (converter == null) {
      throw new RuntimeException("Unsupported logical type: " + logicalType);
    }
    return converter.build(logicalType);
  }
}

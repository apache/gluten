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
package org.apache.gluten;

import org.apache.gluten.proto.IcebergNestedField;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

import java.util.List;

public class IcebergNestedFieldVisitor extends TypeUtil.SchemaVisitor<IcebergNestedField> {

  @Override
  public IcebergNestedField schema(Schema schema, IcebergNestedField fieldResult) {
    return fieldResult;
  }

  @Override
  public IcebergNestedField struct(Types.StructType struct, List<IcebergNestedField> fieldResults) {
    IcebergNestedField.Builder builder = IcebergNestedField.newBuilder();
    for (int i = 0; i < fieldResults.size(); i += 1) {
      builder.addChildren(fieldResults.get(i));
    }
    return builder.build();
  }

  @Override
  public IcebergNestedField field(Types.NestedField field, IcebergNestedField fieldResult) {
    IcebergNestedField.Builder builder = IcebergNestedField.newBuilder();
    builder.setId(field.fieldId());
    if (fieldResult == null) {
      return builder.build();
    }
    return builder.addAllChildren(fieldResult.getChildrenList()).build();
  }

  @Override
  public IcebergNestedField list(Types.ListType list, IcebergNestedField elementResult) {
    IcebergNestedField.Builder elementBuilder = IcebergNestedField.newBuilder();
    elementBuilder.setId(list.elementId());
    if (elementResult != null) {
      elementBuilder.addAllChildren(elementResult.getChildrenList()).build();
    }
    IcebergNestedField.Builder builder = IcebergNestedField.newBuilder();
    builder.addChildren(elementBuilder.build());
    return builder.build();
  }

  @Override
  public IcebergNestedField map(
      Types.MapType map, IcebergNestedField keyResult, IcebergNestedField valueResult) {
    IcebergNestedField.Builder keyBuilder = IcebergNestedField.newBuilder().setId(map.keyId());
    if (keyResult != null) {
      keyBuilder.addAllChildren(keyResult.getChildrenList());
    }
    IcebergNestedField.Builder valueBuilder = IcebergNestedField.newBuilder().setId(map.valueId());
    if (valueResult != null) {
      valueBuilder.addAllChildren(valueResult.getChildrenList());
    }
    IcebergNestedField.Builder builder = IcebergNestedField.newBuilder();

    builder.addChildren(keyBuilder.build());
    builder.addChildren(valueBuilder.build());
    return builder.build();
  }
}

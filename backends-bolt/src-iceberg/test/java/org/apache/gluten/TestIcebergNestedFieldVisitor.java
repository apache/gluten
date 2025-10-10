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
import org.junit.Assert;
import org.junit.Test;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

public class TestIcebergNestedFieldVisitor {

  @Test
  public void testPrimitiveType() {
    Schema schema =
        new Schema(
            required(0, "a", Types.IntegerType.get()), required(1, "A", Types.IntegerType.get()));
    IcebergNestedField protoField = TypeUtil.visit(schema, new IcebergNestedFieldVisitor());
    Assert.assertEquals(2, protoField.getChildrenCount());
    Assert.assertEquals(0, protoField.getChildren(0).getId());
    Assert.assertEquals(1, protoField.getChildren(1).getId());
    System.out.println(protoField.getId());
  }

  @Test
  public void testListType() {
    Schema schema =
        new Schema(
            required(1, "id", Types.IntegerType.get()),
            optional(2, "data", Types.StringType.get()),
            optional(
                3,
                "preferences",
                Types.StructType.of(
                    required(8, "feature1", Types.BooleanType.get()),
                    optional(9, "feature2", Types.BooleanType.get())),
                "struct of named boolean options"),
            required(
                4,
                "locations",
                Types.MapType.ofRequired(
                    10,
                    11,
                    Types.StructType.of(
                        required(20, "address", Types.StringType.get()),
                        required(21, "city", Types.StringType.get()),
                        required(22, "state", Types.StringType.get()),
                        required(23, "zip", Types.IntegerType.get())),
                    Types.StructType.of(
                        required(12, "lat", Types.FloatType.get()),
                        required(13, "long", Types.FloatType.get()))),
                "map of address to coordinate"),
            optional(
                5,
                "points",
                Types.ListType.ofOptional(
                    14,
                    Types.StructType.of(
                        required(15, "x", Types.LongType.get()),
                        required(16, "y", Types.LongType.get()))),
                "2-D cartesian points"),
            required(6, "doubles", Types.ListType.ofRequired(17, Types.DoubleType.get())),
            optional(
                7,
                "properties",
                Types.MapType.ofOptional(18, 19, Types.StringType.get(), Types.StringType.get()),
                "string map of properties"));
    IcebergNestedField protoField = TypeUtil.visit(schema, new IcebergNestedFieldVisitor());
    Assert.assertEquals(7, protoField.getChildrenCount());
    Assert.assertEquals(2, protoField.getChildren(1).getId());
    Assert.assertEquals(3, protoField.getChildren(2).getId());

    IcebergNestedField child = protoField.getChildren(2);
    Assert.assertEquals(2, child.getChildrenCount());
    Assert.assertEquals(8, child.getChildren(0).getId());
    Assert.assertEquals(9, child.getChildren(1).getId());

    child = protoField.getChildren(3);
    Assert.assertEquals(2, child.getChildrenCount());
    Assert.assertEquals(10, child.getChildren(0).getId());
    Assert.assertEquals(11, child.getChildren(1).getId());
    IcebergNestedField child1 = child.getChildren(0);
    Assert.assertEquals(4, child1.getChildrenCount());
    Assert.assertEquals(20, child1.getChildren(0).getId());
    child1 = child.getChildren(1);
    Assert.assertEquals(2, child1.getChildrenCount());
    Assert.assertEquals(12, child1.getChildren(0).getId());

    child = protoField.getChildren(4);
    Assert.assertEquals(1, child.getChildrenCount());
    Assert.assertEquals(14, child.getChildren(0).getId());
    child1 = child.getChildren(0);
    Assert.assertEquals(2, child1.getChildrenCount());
    Assert.assertEquals(15, child1.getChildren(0).getId());
    Assert.assertEquals(16, child1.getChildren(1).getId());

    child = protoField.getChildren(5);
    Assert.assertEquals(1, child.getChildrenCount());
    Assert.assertEquals(17, child.getChildren(0).getId());

    child = protoField.getChildren(6);
    Assert.assertEquals(2, child.getChildrenCount());
    Assert.assertEquals(18, child.getChildren(0).getId());
    Assert.assertEquals(19, child.getChildren(1).getId());
  }
}

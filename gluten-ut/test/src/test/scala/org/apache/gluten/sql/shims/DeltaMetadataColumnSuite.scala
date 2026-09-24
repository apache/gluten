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
package org.apache.gluten.sql.shims

import org.apache.gluten.substrait.`type`.ColumnTypeNode

import org.apache.spark.SparkFunSuite

import io.substrait.proto.NamedStruct

class DeltaMetadataColumnSuite extends SparkFunSuite {
  test("Delta generated fields are not generic Parquet row-index columns") {
    val shims = SparkShimLoader.getSparkShims
    assert(shims.isRowIndexMetadataColumn("_tmp_metadata_row_index"))
    Seq(
      "__delta_internal_row_index",
      "__delta_internal_is_row_deleted",
      "__DELTA_INTERNAL_IS_ROW_DELETED").foreach {
      name => assert(!shims.isRowIndexMetadataColumn(name), name)
    }
  }

  test("Delta deleted-row flags have a distinct Substrait column contract") {
    val column = new ColumnTypeNode(NamedStruct.ColumnType.DELTA_ROW_DELETED_COL)
    val schema = NamedStruct.newBuilder().addColumnTypes(column.toProtobuf()).build()
    val restored = NamedStruct.parseFrom(schema.toByteArray)
    assert(restored.getColumnTypes(0) == NamedStruct.ColumnType.DELTA_ROW_DELETED_COL)
    assert(restored.getColumnTypes(0) != NamedStruct.ColumnType.ROWINDEX_COL)
    assert(NamedStruct.ColumnType.DELTA_ROW_INDEX_COL != NamedStruct.ColumnType.ROWINDEX_COL)
  }
}

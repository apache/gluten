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
package org.apache.gluten.substrait.rel

import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

import io.substrait.proto.Type
import org.scalatest.funsuite.AnyFunSuite

import java.util.{Collections, Map => JMap}

import scala.collection.JavaConverters._

class LocalFilesNodeSuite extends AnyFunSuite {

  private def orcSplit(numFiles: Int): LocalFilesNode = {
    LocalFilesBuilder.makeLocalFiles(
      Integer.valueOf(0),
      (0 until numFiles).map(i => s"file:///tmp/part-$i.orc").asJava,
      Collections.nCopies(numFiles, java.lang.Long.valueOf(0L)),
      Collections.nCopies(numFiles, java.lang.Long.valueOf(100L)),
      Collections.emptyList[java.lang.Long](),
      Collections.emptyList[java.lang.Long](),
      Collections.nCopies[JMap[String, String]](numFiles, Collections.emptyMap[String, String]()),
      Collections.emptyList[JMap[String, String]](),
      LocalFilesNode.ReadFileFormat.OrcReadFormat,
      Collections.emptyList[String](),
      Collections.emptyMap[String, String](),
      Collections.emptyList[JMap[String, Object]]()
    )
  }

  test("every file of a split carries the file schema") {
    val node = orcSplit(3)
    node.setFileSchema(
      StructType(Seq(StructField("Id", LongType), StructField("name", StringType, false))))

    val items = node.toProtobuf.getItemsList.asScala
    assert(items.size == 3)
    items.foreach {
      item =>
        assert(item.hasSchema)
        val schema = item.getSchema
        assert(schema.getNamesList.asScala == Seq("id", "name"))
        val types = schema.getStruct.getTypesList.asScala
        assert(types.map(_.getKindCase) == Seq(Type.KindCase.I64, Type.KindCase.STRING))
        assert(types(0).getI64.getNullability == Type.Nullability.NULLABILITY_NULLABLE)
        assert(types(1).getString.getNullability == Type.Nullability.NULLABILITY_REQUIRED)
    }
  }

  test("no file schema is emitted when none is set") {
    assert(orcSplit(2).toProtobuf.getItemsList.asScala.forall(!_.hasSchema))
  }
}

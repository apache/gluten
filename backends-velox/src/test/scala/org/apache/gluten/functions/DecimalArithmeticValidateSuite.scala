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
package org.apache.gluten.functions

import org.apache.gluten.execution.ProjectExecTransformer

class DecimalArithmeticValidateSuite extends FunctionsValidateSuite {

  import testImplicits._

  // GLUTEN-12260: CheckOverflowTransformer was passing original.child.dataType (Spark's declared
  // type on the raw BinaryArithmetic) instead of child.dataType (the transformer's actual output
  // type, computed by DecimalArithmeticUtil.getResultType after rescaleLiteral).  The mismatch
  // made createCheckOverflowExprNode generate a cast with the wrong source type, causing Velox
  // type validation to fail and ColumnarPartialProjectRule to fall back the entire Project to JVM.
  test("GLUTEN-12260: bigint aggregate divided by integer-valued decimal literal stays native") {
    val t1 = Seq(200L).toDF("val")
    val t2 = Seq(100L, 100L, 100L, 100L, 100L).toDF("val")
    withTempView("t1_dec", "t2_dec") {
      t1.createOrReplaceTempView("t1_dec")
      t2.createOrReplaceTempView("t2_dec")
      runQueryAndCompare(
        """
          |SELECT a.val,
          |       (a.val - COALESCE(SUM(b.val), 0) / 5.0)
          |           / (COALESCE(SUM(b.val), 0) / 5.0) AS growth_rate
          |FROM t1_dec a CROSS JOIN t2_dec b
          |GROUP BY a.val
          |""".stripMargin
      ) {
        checkGlutenPlan[ProjectExecTransformer]
      }
    }
  }
}

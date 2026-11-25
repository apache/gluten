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
package org.apache.spark.sql.execution.python

import org.apache.spark.sql.{AnalysisException, GlutenSQLTestsTrait, IntegratedUDFTestUtils}
import org.apache.spark.sql.functions.{array, transform}

class GlutenPythonUDFSuite extends PythonUDFSuite with GlutenSQLTestsTrait {

  // Override: the original test uses this.getClass.getSimpleName in ExpectedContext pattern,
  // which returns "GlutenPythonUDFSuite" but the actual callSite records "PythonUDFSuite".
  testGluten("SPARK-48706: Negative test case for Python UDF in higher order functions") {
    assume(IntegratedUDFTestUtils.shouldTestPythonUDFs)
    checkError(
      exception = intercept[AnalysisException] {
        spark.range(1).select(transform(array("id"), x => pythonTestUDF(x))).collect()
      },
      condition = "UNSUPPORTED_FEATURE.LAMBDA_FUNCTION_WITH_PYTHON_UDF",
      parameters = Map("funcName" -> "\"pyUDF(namedlambdavariable())\""),
      context = ExpectedContext("transform", s".*PythonUDFSuite.*")
    )
  }
}

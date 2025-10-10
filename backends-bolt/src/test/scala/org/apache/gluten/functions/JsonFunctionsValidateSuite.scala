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

import org.apache.spark.sql.execution.ProjectExec

class JsonFunctionsValidateSuite extends FunctionsValidateSuite {

  disableFallbackCheck
  import testImplicits._

  test("get_json_object") {
    runQueryAndCompare(
      "SELECT get_json_object(string_field1, '$.a') " +
        "from datatab limit 1;") {
      checkGlutenPlan[ProjectExecTransformer]
    }

    withTempPath {
      path =>
        Seq[String](
          """{"a":"b"}"""
        )
          .toDF("txt")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("tbl")

        runQueryAndCompare("select get_json_object(txt, '$.a') from tbl") {
          checkGlutenPlan[ProjectExecTransformer]
        }
    }

    // Invalid UTF-8 encoding.
    spark.sql(
      "CREATE TABLE t USING parquet SELECT concat('{\"a\": 2, \"'," +
        " string(X'80'), '\": 3, \"c\": 100}') AS c1")
    withTable("t") {
      runQueryAndCompare("SELECT get_json_object(c1, '$.c') FROM t;") {
        checkGlutenPlan[ProjectExecTransformer]
      }
    }
  }

  test("json_array_length") {
    runQueryAndCompare(
      s"select *, json_array_length(string_field1) " +
        s"from datatab limit 5")(checkGlutenPlan[ProjectExecTransformer])
    withTempPath {
      path =>
        Seq[String](
          "[1,2,3,4]",
          null.asInstanceOf[String]
        )
          .toDF("txt")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("tbl")

        runQueryAndCompare("select json_array_length(txt) from tbl") {
          checkGlutenPlan[ProjectExecTransformer]
        }
    }
  }

  testWithMinSparkVersion("from_json function bool", "3.4") {
    withTempPath {
      path =>
        Seq[String](
          """{"id":0}""",
          """{"id":0.0}""",
          """{"id":true}""",
          """{"id":false}""",
          """{"id":"true"}"""
        )
          .toDF("txt")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("tbl")

        runQueryAndCompare("select txt, from_json(txt, 'id BOOLEAN') from tbl") {
          checkGlutenPlan[ProjectExecTransformer]
        }
    }
  }

  testWithMinSparkVersion("from_json function small int", "3.4") {
    withTempPath {
      path =>
        Seq[String](
          """{"id":100000000}""",
          """{"id":11.0}""",
          """{"id":'true'}""",
          """{"id":true}""",
          """{"id":'12'}"""
        )
          .toDF("txt")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("tbl")

        runQueryAndCompare("select txt, from_json(txt, 'id SHORT') from tbl") {
          checkGlutenPlan[ProjectExecTransformer]
        }
    }
  }

  testWithMinSparkVersion("from_json function int", "3.4") {
    withTempPath {
      path =>
        Seq[String](
          """{"id":10}""",
          """{"id":11.0}""",
          """{"id":"true"}""",
          """{"id":true}""",
          """{"id":"12"}"""
        )
          .toDF("txt")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("tbl")

        runQueryAndCompare("select txt, from_json(txt, 'id INT') from tbl") {
          checkGlutenPlan[ProjectExecTransformer]
        }
    }
  }

  testWithMinSparkVersion("from_json function big int", "3.4") {
    withTempPath {
      path =>
        Seq[String](
          """{"id":100000000}""",
          """{"id":11.0}""",
          """{"id":'true'}""",
          """{"id":true}""",
          """{"id":'12'}"""
        )
          .toDF("txt")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("tbl")

        runQueryAndCompare("select txt, from_json(txt, 'id LONG') from tbl") {
          checkGlutenPlan[ProjectExecTransformer]
        }
    }
  }

  testWithMinSparkVersion("from_json function float", "3.4") {
    withTempPath {
      path =>
        Seq[String](
          """{"id":10}""",
          """{"id":11.0}""",
          """{"id":"12.0"}""",
          """{"id":"test"}""",
          """{"id":"12"}""",
          """{"id":"-INF"}""",
          """{"id":"NaN"}"""
        )
          .toDF("txt")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("tbl")

        runQueryAndCompare("select txt, from_json(txt, 'id FLOAT') from tbl") {
          checkGlutenPlan[ProjectExecTransformer]
        }
    }
  }

  testWithMinSparkVersion("from_json function double", "3.4") {
    withTempPath {
      path =>
        Seq[String](
          """{"id":10}""",
          """{"id":11.0}""",
          """{"id":"12.0"}""",
          """{"id":"test"}""",
          """{"id":"12"}""",
          """{"id":"+INF"}""",
          """{"id":"NaN"}"""
        )
          .toDF("txt")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("tbl")

        runQueryAndCompare("select txt, from_json(txt, 'id DOUBLE') from tbl") {
          checkGlutenPlan[ProjectExecTransformer]
        }
    }
  }

  testWithMinSparkVersion("from_json function string", "3.4") {
    withTempPath {
      path =>
        Seq[String](
          """{"id":10}""",
          """{"id":false}""",
          """{"id":"00010"}""",
          """{"id":[1,2]}""",
          """{"id":{"a":1}}"""
        )
          .toDF("txt")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("tbl")

        runQueryAndCompare("select txt, from_json(txt, 'id STRING') from tbl") {
          checkGlutenPlan[ProjectExecTransformer]
        }
    }
  }

  testWithMinSparkVersion("from_json function array", "3.4") {
    withTempPath {
      path =>
        Seq[String](
          """[]""",
          """[1, 3]""",
          """[1, 2, 3.0]"""
        )
          .toDF("txt")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("tbl")

        runQueryAndCompare("select from_json(txt, 'array<int>') from tbl") {
          checkGlutenPlan[ProjectExecTransformer]
        }
    }
  }

  testWithMinSparkVersion("from_json function map", "3.4") {
    withTempPath {
      path =>
        Seq[String](
          """{"id":10, "value":11}""",
          """{"id":11, "value":11.0}""",
          """{"id":10, "Id":11}""",
          """{4:10, "Id":11}""",
          """{}"""
        )
          .toDF("txt")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("tbl")

        runQueryAndCompare("select from_json(txt, 'map<string,int>') from tbl") {
          checkGlutenPlan[ProjectExecTransformer]
        }
    }
  }

  testWithMinSparkVersion("from_json function row", "3.4") {
    withTempPath {
      path =>
        Seq[String](
          """{"Id":"10", "Value":"11"}""",
          """{"Id":"11", "Value":"11.0"}""",
          """{"Id":"10", "Value":"11"}"""
        )
          .toDF("txt")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("tbl")

        runQueryAndCompare("select from_json(txt, 'Id STRING, Value STRING') from tbl") {
          checkGlutenPlan[ProjectExecTransformer]
        }
    }
  }

  test("from_json function CORRUPT_RECORD") {
    withTempPath {
      path =>
        Seq[String](
          """{"id":00010}""",
          """{"id":1.0}"""
        )
          .toDF("txt")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("tbl")

        runQueryAndCompare(
          "select txt, from_json(txt, 'id INT, _corrupt_record STRING') from tbl") {
          checkSparkPlan[ProjectExec]
        }
    }
  }

  testWithMinSparkVersion("from_json function duplicate key", "3.4") {
    withTempPath {
      path =>
        Seq[String](
          """{"id":1,"Id":2}""",
          """{"id":3,"Id":4}"""
        )
          .toDF("txt")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("tbl")

        runQueryAndCompare("select txt, from_json(txt, 'id INT, Id INT') from tbl") {
          checkSparkPlan[ProjectExec]
        }

        runQueryAndCompare("select txt, from_json(txt, 'id INT, id INT') from tbl") {
          checkSparkPlan[ProjectExec]
        }

        runQueryAndCompare("select txt, from_json(txt, 'id INT') from tbl") {
          checkSparkPlan[ProjectExecTransformer]
        }
    }
  }

  test("json_object_keys") {
    withTempPath {
      path =>
        Seq[String](
          """""",
          """200""",
          """{}""",
          """{"key": 1}""",
          """{"key": "value", "key2": 2}""",
          """{"arrayKey": [1, 2, 3]}""",
          """{"key":[1,2,3,{"key":"value"},[1,2,3]]}""",
          """{"f1":"abc","f2":{"f3":"a", "f4":"b"}}""",
          """{"k1": [1, 2, {"key": 5}], "k2": {"key2": [1, 2]}}""",
          """[1, 2, 3]""",
          """{[1,2]}""",
          """{"key": 45, "random_string"}""",
          null.asInstanceOf[String]
        )
          .toDF("txt")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("tbl")

        runQueryAndCompare("select txt, json_object_keys(txt) from tbl") {
          checkSparkPlan[ProjectExecTransformer]
        }
    }
  }

  test("to_json function") {
    withTable("t") {
      spark.sql(
        """
          |create table t (a int, b string, c array<int>, d map<int, string>, e struct<aA: int>)
          |using parquet
          |""".stripMargin)
      spark.sql("""insert into t values (1, 'str', array(1,2,3), map(1, 'v'), struct(1)),
                  |(2, 'str2', array(), map(1, 'v1', 2, 'v2'), struct(2)),
                  |(3, '', array(1), map(), struct(null))
                  |""".stripMargin)

      runQueryAndCompare("select to_json(named_struct('a', a, 'b', b, 'c', c, 'd', d)) from t") {
        checkGlutenPlan[ProjectExecTransformer]
      }

      runQueryAndCompare("select to_json(c) from t") {
        checkGlutenPlan[ProjectExecTransformer]
      }

      runQueryAndCompare("select to_json(d) from t") {
        checkGlutenPlan[ProjectExecTransformer]
      }

      runQueryAndCompare("select to_json(e) from t") {
        checkSparkPlan[ProjectExec]
      }

      runQueryAndCompare("select to_json(Array(named_struct('aA', a))) from t") {
        checkSparkPlan[ProjectExec]
      }
    }
  }
}

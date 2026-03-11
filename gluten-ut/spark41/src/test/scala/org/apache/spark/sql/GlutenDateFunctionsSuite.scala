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
package org.apache.spark.sql

import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf

import java.sql.{Date, Timestamp}
import java.time.{LocalDateTime, ZoneId}
import java.util.concurrent.TimeUnit

class GlutenDateFunctionsSuite extends DateFunctionsSuite with GlutenSQLTestsTrait {
  import testImplicits._

  private def secs(millis: Long): Long = TimeUnit.MILLISECONDS.toSeconds(millis)

  testGluten("unix_timestamp") {
    Seq("corrected", "legacy").foreach {
      legacyParserPolicy =>
        withSQLConf(
          SQLConf.LEGACY_TIME_PARSER_POLICY.key -> legacyParserPolicy,
          SQLConf.SESSION_LOCAL_TIMEZONE.key -> ZoneId.systemDefault().toString) {
          val date1 = Date.valueOf("2015-07-24")
          val date2 = Date.valueOf("2015-07-25")
          val ts1 = Timestamp.valueOf("2015-07-24 10:00:00.3")
          val ts2 = Timestamp.valueOf("2015-07-25 02:02:02.2")
          val ntzTs1 = LocalDateTime.parse("2015-07-24T10:00:00.3")
          val ntzTs2 = LocalDateTime.parse("2015-07-25T02:02:02.2")
          val s1 = "2015/07/24 10:00:00.5"
          val s2 = "2015/07/25 02:02:02.6"
          val ss1 = "2015-07-24 10:00:00"
          val ss2 = "2015-07-25 02:02:02"
          val fmt = "yyyy/MM/dd HH:mm:ss.S"
          val df = Seq((date1, ts1, ntzTs1, s1, ss1), (date2, ts2, ntzTs2, s2, ss2)).toDF(
            "d",
            "ts",
            "ntzTs",
            "s",
            "ss")
          checkAnswer(
            df.select(unix_timestamp(col("ts"))),
            Seq(Row(secs(ts1.getTime)), Row(secs(ts2.getTime))))
          checkAnswer(
            df.select(unix_timestamp(col("ss"))),
            Seq(Row(secs(ts1.getTime)), Row(secs(ts2.getTime))))
          checkAnswer(
            df.select(unix_timestamp(col("ntzTs"))),
            Seq(
              Row(secs(DateTimeUtils.microsToMillis(DateTimeUtils.localDateTimeToMicros(ntzTs1)))),
              Row(secs(DateTimeUtils.microsToMillis(DateTimeUtils.localDateTimeToMicros(ntzTs2))))
            )
          )
          checkAnswer(
            df.select(unix_timestamp(col("d"), fmt)),
            Seq(Row(secs(date1.getTime)), Row(secs(date2.getTime))))
          checkAnswer(
            df.select(unix_timestamp(col("s"), fmt)),
            Seq(Row(secs(ts1.getTime)), Row(secs(ts2.getTime))))
          checkAnswer(
            df.selectExpr("unix_timestamp(ts)"),
            Seq(Row(secs(ts1.getTime)), Row(secs(ts2.getTime))))
          checkAnswer(
            df.selectExpr("unix_timestamp(ss)"),
            Seq(Row(secs(ts1.getTime)), Row(secs(ts2.getTime))))
          checkAnswer(
            df.selectExpr("unix_timestamp(ntzTs)"),
            Seq(
              Row(secs(DateTimeUtils.microsToMillis(DateTimeUtils.localDateTimeToMicros(ntzTs1)))),
              Row(secs(DateTimeUtils.microsToMillis(DateTimeUtils.localDateTimeToMicros(ntzTs2))))
            )
          )
          checkAnswer(
            df.selectExpr(s"unix_timestamp(d, '$fmt')"),
            Seq(Row(secs(date1.getTime)), Row(secs(date2.getTime))))
          checkAnswer(
            df.selectExpr(s"unix_timestamp(s, '$fmt')"),
            Seq(Row(secs(ts1.getTime)), Row(secs(ts2.getTime))))

          val x1 = "2015-07-24 10:00:00"
          val x2 = "2015-25-07 02:02:02"
          val x3 = "2015-07-24 25:02:02"
          val x4 = "2015-24-07 26:02:02"
          val ts3 = Timestamp.valueOf("2015-07-24 02:25:02")
          val ts4 = Timestamp.valueOf("2015-07-24 00:10:00")

          val df1 = Seq(x1, x2, x3, x4).toDF("x")
          checkAnswer(
            df1.select(unix_timestamp(col("x"))),
            Seq(Row(secs(ts1.getTime)), Row(null), Row(null), Row(null)))
          checkAnswer(
            df1.selectExpr("unix_timestamp(x)"),
            Seq(Row(secs(ts1.getTime)), Row(null), Row(null), Row(null)))
          checkAnswer(
            df1.select(unix_timestamp(col("x"), "yyyy-dd-MM HH:mm:ss")),
            Seq(Row(null), Row(secs(ts2.getTime)), Row(null), Row(null)))
          checkAnswer(
            df1.selectExpr(s"unix_timestamp(x, 'yyyy-MM-dd mm:HH:ss')"),
            Seq(Row(secs(ts4.getTime)), Row(null), Row(secs(ts3.getTime)), Row(null)))

          // invalid format
          val invalid = df1.selectExpr(s"unix_timestamp(x, 'yyyy-MM-dd aa:HH:ss')")
          checkAnswer(invalid, Seq(Row(null), Row(null), Row(null), Row(null)))

          // February
          val y1 = "2016-02-29"
          val y2 = "2017-02-29"
          val ts5 = Timestamp.valueOf("2016-02-29 00:00:00")
          val df2 = Seq(y1, y2).toDF("y")
          checkAnswer(
            df2.select(unix_timestamp(col("y"), "yyyy-MM-dd")),
            Seq(Row(secs(ts5.getTime)), Row(null)))

          // Test unix_timestamp(timestamp, format) - format should be ignored
          checkAnswer(
            df.select(unix_timestamp(col("ts"), "yyyy-MM-dd")),
            Seq(Row(secs(ts1.getTime)), Row(secs(ts2.getTime))))
          checkAnswer(
            df.selectExpr("unix_timestamp(ts, 'invalid-format')"),
            Seq(Row(secs(ts1.getTime)), Row(secs(ts2.getTime))))

          val now = sql("select unix_timestamp()").collect().head.getLong(0)
          checkAnswer(
            sql(s"select timestamp_seconds($now)"),
            Row(new java.util.Date(TimeUnit.SECONDS.toMillis(now))))
        }
    }
  }

  testGluten("to_unix_timestamp") {
    Seq("corrected", "legacy").foreach {
      legacyParserPolicy =>
        withSQLConf(
          SQLConf.LEGACY_TIME_PARSER_POLICY.key -> legacyParserPolicy,
          SQLConf.SESSION_LOCAL_TIMEZONE.key -> ZoneId.systemDefault().toString
        ) {
          val date1 = Date.valueOf("2015-07-24")
          val date2 = Date.valueOf("2015-07-25")
          val ts1 = Timestamp.valueOf("2015-07-24 10:00:00.3")
          val ts2 = Timestamp.valueOf("2015-07-25 02:02:02.2")
          val s1 = "2015/07/24 10:00:00.5"
          val s2 = "2015/07/25 02:02:02.6"
          val ss1 = "2015-07-24 10:00:00"
          val ss2 = "2015-07-25 02:02:02"
          val fmt = "yyyy/MM/dd HH:mm:ss.S"
          val df = Seq((date1, ts1, s1, ss1), (date2, ts2, s2, ss2)).toDF("d", "ts", "s", "ss")
          checkAnswer(
            df.selectExpr("to_unix_timestamp(ts)"),
            Seq(Row(secs(ts1.getTime)), Row(secs(ts2.getTime))))
          checkAnswer(
            df.selectExpr("to_unix_timestamp(ss)"),
            Seq(Row(secs(ts1.getTime)), Row(secs(ts2.getTime))))
          checkAnswer(
            df.selectExpr(s"to_unix_timestamp(d, '$fmt')"),
            Seq(Row(secs(date1.getTime)), Row(secs(date2.getTime))))
          checkAnswer(
            df.selectExpr(s"to_unix_timestamp(s, '$fmt')"),
            Seq(Row(secs(ts1.getTime)), Row(secs(ts2.getTime))))

          val x1 = "2015-07-24 10:00:00"
          val x2 = "2015-25-07 02:02:02"
          val x3 = "2015-07-24 25:02:02"
          val x4 = "2015-24-07 26:02:02"
          val ts3 = Timestamp.valueOf("2015-07-24 02:25:02")
          val ts4 = Timestamp.valueOf("2015-07-24 00:10:00")

          val df1 = Seq(x1, x2, x3, x4).toDF("x")
          checkAnswer(
            df1.selectExpr("to_unix_timestamp(x)"),
            Seq(Row(secs(ts1.getTime)), Row(null), Row(null), Row(null)))
          checkAnswer(
            df1.selectExpr(s"to_unix_timestamp(x, 'yyyy-MM-dd mm:HH:ss')"),
            Seq(Row(secs(ts4.getTime)), Row(null), Row(secs(ts3.getTime)), Row(null)))

          // February
          val y1 = "2016-02-29"
          val y2 = "2017-02-29"
          val ts5 = Timestamp.valueOf("2016-02-29 00:00:00")
          val df2 = Seq(y1, y2).toDF("y")
          checkAnswer(
            df2.select(unix_timestamp(col("y"), "yyyy-MM-dd")),
            Seq(Row(secs(ts5.getTime)), Row(null)))

          // Test to_unix_timestamp(timestamp, format) - format should be ignored
          checkAnswer(
            df.selectExpr("to_unix_timestamp(ts, 'yyyy-MM-dd')"),
            Seq(Row(secs(ts1.getTime)), Row(secs(ts2.getTime))))
          checkAnswer(
            df.selectExpr("to_unix_timestamp(ts, 'invalid-format')"),
            Seq(Row(secs(ts1.getTime)), Row(secs(ts2.getTime))))

          val invalid = df1.selectExpr(s"to_unix_timestamp(x, 'yyyy-MM-dd bb:HH:ss')")
          checkAnswer(invalid, Seq(Row(null), Row(null), Row(null), Row(null)))
        }
    }
  }

  testGluten("function to_date") {
    val d1 = Date.valueOf("2015-07-22")
    val d2 = Date.valueOf("2015-07-01")
    val d3 = Date.valueOf("2014-12-31")
    val t1 = Timestamp.valueOf("2015-07-22 10:00:00")
    val t2 = Timestamp.valueOf("2014-12-31 23:59:59")
    val t3 = Timestamp.valueOf("2014-12-31 23:59:59")
    val s1 = "2015-07-22 10:00:00"
    val s2 = "2014-12-31"
    val s3 = "2014-31-12"
    val df = Seq((d1, t1, s1), (d2, t2, s2), (d3, t3, s3)).toDF("d", "t", "s")

    checkAnswer(
      df.select(to_date(col("t"))),
      Seq(
        Row(Date.valueOf("2015-07-22")),
        Row(Date.valueOf("2014-12-31")),
        Row(Date.valueOf("2014-12-31"))))
    checkAnswer(
      df.select(to_date(col("d"))),
      Seq(
        Row(Date.valueOf("2015-07-22")),
        Row(Date.valueOf("2015-07-01")),
        Row(Date.valueOf("2014-12-31"))))
    checkAnswer(
      df.select(to_date(col("s"))),
      Seq(Row(Date.valueOf("2015-07-22")), Row(Date.valueOf("2014-12-31")), Row(null)))

    checkAnswer(
      df.selectExpr("to_date(t)"),
      Seq(
        Row(Date.valueOf("2015-07-22")),
        Row(Date.valueOf("2014-12-31")),
        Row(Date.valueOf("2014-12-31"))))
    checkAnswer(
      df.selectExpr("to_date(d)"),
      Seq(
        Row(Date.valueOf("2015-07-22")),
        Row(Date.valueOf("2015-07-01")),
        Row(Date.valueOf("2014-12-31"))))
    checkAnswer(
      df.selectExpr("to_date(s)"),
      Seq(Row(Date.valueOf("2015-07-22")), Row(Date.valueOf("2014-12-31")), Row(null)))

    // now with format
    checkAnswer(
      df.select(to_date(col("t"), "yyyy-MM-dd")),
      Seq(
        Row(Date.valueOf("2015-07-22")),
        Row(Date.valueOf("2014-12-31")),
        Row(Date.valueOf("2014-12-31"))))
    checkAnswer(
      df.select(to_date(col("d"), "yyyy-MM-dd")),
      Seq(
        Row(Date.valueOf("2015-07-22")),
        Row(Date.valueOf("2015-07-01")),
        Row(Date.valueOf("2014-12-31"))))
    val confKey = SQLConf.LEGACY_TIME_PARSER_POLICY.key
    withSQLConf(confKey -> "corrected") {
      checkAnswer(
        df.select(to_date(col("s"), "yyyy-MM-dd")),
        Seq(Row(null), Row(Date.valueOf("2014-12-31")), Row(null)))
    }
    // legacyParserPolicy is not respected by Gluten.
    // withSQLConf(confKey -> "exception") {
    //   checkExceptionMessage(df.select(to_date(col("s"), "yyyy-MM-dd")))
    // }

    // now switch format
    checkAnswer(
      df.select(to_date(col("s"), "yyyy-dd-MM")),
      Seq(Row(null), Row(null), Row(Date.valueOf("2014-12-31"))))

    // invalid format
    checkAnswer(df.select(to_date(col("s"), "yyyy-hh-MM")), Seq(Row(null), Row(null), Row(null)))
    // velox getTimestamp function does not throw exception when format is "yyyy-dd-aa".
    // val e =
    //   intercept[SparkUpgradeException](df.select(to_date(col("s"), "yyyy-dd-aa")).collect())
    // assert(e.getCause.isInstanceOf[IllegalArgumentException])
    // assert(
    //   e.getMessage.contains("You may get a different result due to the upgrading to Spark"))

    // February
    val x1 = "2016-02-29"
    val x2 = "2017-02-29"
    val df1 = Seq(x1, x2).toDF("x")
    checkAnswer(df1.select(to_date(col("x"))), Row(Date.valueOf("2016-02-29")) :: Row(null) :: Nil)
  }
}

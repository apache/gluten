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
package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.GlutenTestsTrait
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.catalyst.util.DateTimeConstants._
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils._
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.catalyst.util.DateTimeUtils.{getZoneId, TimeZoneUTC}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat
import java.time.{LocalDateTime, ZoneId}
import java.util.{Calendar, Locale, TimeZone}
import java.util.concurrent.TimeUnit._

class GlutenDateExpressionsSuite extends DateExpressionsSuite with GlutenTestsTrait {
  override def testIntegralInput(testFunc: Number => Unit): Unit = {
    def checkResult(input: Long): Unit = {
      if (input.toByte == input) {
        testFunc(input.toByte)
      } else if (input.toShort == input) {
        testFunc(input.toShort)
      } else if (input.toInt == input) {
        testFunc(input.toInt)
      } else {
        testFunc(input)
      }
    }

    checkResult(0)
    checkResult(Byte.MaxValue)
    checkResult(Byte.MinValue)
    checkResult(Short.MaxValue)
    checkResult(Short.MinValue)
    // Spark collect causes integer overflow.
    // checkResult(Int.MaxValue)
    // checkResult(Int.MinValue)
    // checkResult(Int.MaxValue.toLong + 100)
    // checkResult(Int.MinValue.toLong - 100)
  }

  testGluten("TIMESTAMP_MICROS") {
    def testIntegralFunc(value: Number): Unit = {
      checkEvaluation(MicrosToTimestamp(Literal(value)), value.longValue())
    }

    // test null input
    checkEvaluation(MicrosToTimestamp(Literal(null, IntegerType)), null)

    // test integral input
    testIntegralInput(testIntegralFunc)
    // test max/min input
    // Spark collect causes long overflow.
    // testIntegralFunc(Long.MaxValue)
    // testIntegralFunc(Long.MinValue)
  }

  val outstandingTimezonesIds: Seq[String] = Seq(
    // Velox doesn't support timezones like UTC.
    // "UTC",
    PST.getId,
    CET.getId,
    "Africa/Dakar",
    LA.getId,
    "Asia/Urumqi",
    "Asia/Hong_Kong",
    "Europe/Brussels")
  val outstandingZoneIds: Seq[ZoneId] = outstandingTimezonesIds.map(getZoneId)

  testGluten("unix_timestamp") {
    Seq("legacy", "corrected").foreach {
      legacyParserPolicy =>
        withDefaultTimeZone(UTC) {
          for (zid <- outstandingZoneIds) {
            withSQLConf(
              SQLConf.LEGACY_TIME_PARSER_POLICY.key -> legacyParserPolicy,
              SQLConf.SESSION_LOCAL_TIMEZONE.key -> zid.getId
            ) {
              val sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US)
              val fmt2 = "yyyy-MM-dd HH:mm:ss.SSS"
              val sdf2 = new SimpleDateFormat(fmt2, Locale.US)
              val fmt3 = "yy-MM-dd"
              val sdf3 = new SimpleDateFormat(fmt3, Locale.US)
              sdf3.setTimeZone(TimeZoneUTC)

              val timeZoneId = Option(zid.getId)
              val tz = TimeZone.getTimeZone(zid)
              sdf1.setTimeZone(tz)
              sdf2.setTimeZone(tz)

              val date1 = Date.valueOf("2015-07-24")
              checkEvaluation(
                UnixTimestamp(
                  Literal(sdf1.format(new Timestamp(0))),
                  Literal("yyyy-MM-dd HH:mm:ss"),
                  timeZoneId),
                0L)
              checkEvaluation(
                UnixTimestamp(
                  Literal(sdf1.format(new Timestamp(1000000))),
                  Literal("yyyy-MM-dd HH:mm:ss"),
                  timeZoneId),
                1000L)
              checkEvaluation(
                UnixTimestamp(
                  Literal(new Timestamp(1000000)),
                  Literal("yyyy-MM-dd HH:mm:ss"),
                  timeZoneId),
                1000L)
              checkEvaluation(
                UnixTimestamp(
                  Literal(
                    DateTimeUtils.microsToLocalDateTime(DateTimeUtils.millisToMicros(1000000))),
                  Literal("yyyy-MM-dd HH:mm:ss"),
                  timeZoneId),
                1000L)
              checkEvaluation(
                UnixTimestamp(Literal(date1), Literal("yyyy-MM-dd HH:mm:ss"), timeZoneId),
                MICROSECONDS.toSeconds(
                  DateTimeUtils.daysToMicros(DateTimeUtils.fromJavaDate(date1), tz.toZoneId))
              )
              checkEvaluation(
                UnixTimestamp(
                  Literal(sdf2.format(new Timestamp(-1000000))),
                  Literal(fmt2),
                  timeZoneId),
                -1000L)
              checkEvaluation(
                UnixTimestamp(
                  Literal(sdf3.format(Date.valueOf("2015-07-24"))),
                  Literal(fmt3),
                  timeZoneId),
                MICROSECONDS.toSeconds(
                  DateTimeUtils.daysToMicros(
                    DateTimeUtils.fromJavaDate(Date.valueOf("2015-07-24")),
                    tz.toZoneId))
              )
              checkEvaluation(
                UnixTimestamp(
                  Literal.create(null, DateType),
                  Literal.create(null, StringType),
                  timeZoneId),
                null)
              checkEvaluation(
                UnixTimestamp(
                  Literal.create(null, DateType),
                  Literal("yyyy-MM-dd HH:mm:ss"),
                  timeZoneId),
                null)
              checkEvaluation(
                UnixTimestamp(Literal(date1), Literal.create(null, StringType), timeZoneId),
                MICROSECONDS.toSeconds(
                  DateTimeUtils.daysToMicros(DateTimeUtils.fromJavaDate(date1), tz.toZoneId))
              )
            }
          }
        }
    }
    // Test escaping of format
    GenerateUnsafeProjection.generate(
      UnixTimestamp(Literal("2015-07-24"), Literal("\""), UTC_OPT) :: Nil)
  }

  testGluten("to_unix_timestamp") {
    withDefaultTimeZone(UTC) {
      for (zid <- outstandingZoneIds) {
        Seq("legacy", "corrected").foreach {
          legacyParserPolicy =>
            withSQLConf(
              SQLConf.LEGACY_TIME_PARSER_POLICY.key -> legacyParserPolicy,
              SQLConf.SESSION_LOCAL_TIMEZONE.key -> zid.getId
            ) {
              val fmt1 = "yyyy-MM-dd HH:mm:ss"
              val sdf1 = new SimpleDateFormat(fmt1, Locale.US)
              val fmt2 = "yyyy-MM-dd HH:mm:ss.SSS"
              val sdf2 = new SimpleDateFormat(fmt2, Locale.US)
              val fmt3 = "yy-MM-dd"
              val sdf3 = new SimpleDateFormat(fmt3, Locale.US)
              sdf3.setTimeZone(TimeZoneUTC)

              val timeZoneId = Option(zid.getId)
              val tz = TimeZone.getTimeZone(zid)
              sdf1.setTimeZone(tz)
              sdf2.setTimeZone(tz)

              val date1 = Date.valueOf("2015-07-24")
              checkEvaluation(
                ToUnixTimestamp(Literal(sdf1.format(new Timestamp(0))), Literal(fmt1), timeZoneId),
                0L)
              checkEvaluation(
                ToUnixTimestamp(
                  Literal(sdf1.format(new Timestamp(1000000))),
                  Literal(fmt1),
                  timeZoneId),
                1000L)
              checkEvaluation(
                ToUnixTimestamp(Literal(new Timestamp(1000000)), Literal(fmt1)),
                1000L)
              checkEvaluation(
                ToUnixTimestamp(
                  Literal(
                    DateTimeUtils.microsToLocalDateTime(DateTimeUtils.millisToMicros(1000000))),
                  Literal(fmt1)),
                1000L)
              checkEvaluation(
                ToUnixTimestamp(Literal(date1), Literal(fmt1), timeZoneId),
                MICROSECONDS.toSeconds(
                  DateTimeUtils.daysToMicros(DateTimeUtils.fromJavaDate(date1), zid)))
              checkEvaluation(
                ToUnixTimestamp(
                  Literal(sdf2.format(new Timestamp(-1000000))),
                  Literal(fmt2),
                  timeZoneId),
                -1000L)
              checkEvaluation(
                ToUnixTimestamp(
                  Literal(sdf3.format(Date.valueOf("2015-07-24"))),
                  Literal(fmt3),
                  timeZoneId),
                MICROSECONDS.toSeconds(DateTimeUtils
                  .daysToMicros(DateTimeUtils.fromJavaDate(Date.valueOf("2015-07-24")), zid))
              )
              checkEvaluation(
                ToUnixTimestamp(
                  Literal.create(null, DateType),
                  Literal.create(null, StringType),
                  timeZoneId),
                null)
              checkEvaluation(
                ToUnixTimestamp(Literal.create(null, DateType), Literal(fmt1), timeZoneId),
                null)
              checkEvaluation(
                ToUnixTimestamp(Literal(date1), Literal.create(null, StringType), timeZoneId),
                MICROSECONDS.toSeconds(
                  DateTimeUtils.daysToMicros(DateTimeUtils.fromJavaDate(date1), zid))
              )

              // SPARK-28072 The codegen path for non-literal input should also work
              checkEvaluation(
                expression = ToUnixTimestamp(
                  BoundReference(ordinal = 0, dataType = StringType, nullable = true),
                  BoundReference(ordinal = 1, dataType = StringType, nullable = true),
                  timeZoneId),
                expected = 0L,
                inputRow = InternalRow(
                  UTF8String.fromString(sdf1.format(new Timestamp(0))),
                  UTF8String.fromString(fmt1))
              )
            }
        }
      }
    }
    // Test escaping of format
    GenerateUnsafeProjection.generate(
      ToUnixTimestamp(Literal("2015-07-24"), Literal("\""), UTC_OPT) :: Nil)
  }

  // Modified based on vanilla spark to explicitly set timezone in config.
  testGluten("DateFormat") {
    val PST_OPT = Option("America/Los_Angeles")
    val JST_OPT = Option("Asia/Tokyo")

    Seq("legacy", "corrected").foreach {
      legacyParserPolicy =>
        withSQLConf(
          SQLConf.LEGACY_TIME_PARSER_POLICY.key -> legacyParserPolicy,
          SQLConf.SESSION_LOCAL_TIMEZONE.key -> UTC_OPT.get) {
          checkEvaluation(
            DateFormatClass(Literal.create(null, TimestampType), Literal("y"), UTC_OPT),
            null)
          checkEvaluation(
            DateFormatClass(
              Cast(Literal(d), TimestampType, UTC_OPT),
              Literal.create(null, StringType),
              UTC_OPT),
            null)

          checkEvaluation(
            DateFormatClass(Cast(Literal(d), TimestampType, UTC_OPT), Literal("y"), UTC_OPT),
            "2015")
          checkEvaluation(DateFormatClass(Literal(ts), Literal("y"), UTC_OPT), "2013")
          checkEvaluation(
            DateFormatClass(Cast(Literal(d), TimestampType, UTC_OPT), Literal("H"), UTC_OPT),
            "0")
          checkEvaluation(DateFormatClass(Literal(ts), Literal("H"), UTC_OPT), "13")
        }

        withSQLConf(
          SQLConf.LEGACY_TIME_PARSER_POLICY.key -> legacyParserPolicy,
          SQLConf.SESSION_LOCAL_TIMEZONE.key -> PST_OPT.get) {
          checkEvaluation(
            DateFormatClass(Cast(Literal(d), TimestampType, PST_OPT), Literal("y"), PST_OPT),
            "2015")
          checkEvaluation(DateFormatClass(Literal(ts), Literal("y"), PST_OPT), "2013")
          checkEvaluation(
            DateFormatClass(Cast(Literal(d), TimestampType, PST_OPT), Literal("H"), PST_OPT),
            "0")
          checkEvaluation(DateFormatClass(Literal(ts), Literal("H"), PST_OPT), "5")
        }

        withSQLConf(
          SQLConf.LEGACY_TIME_PARSER_POLICY.key -> legacyParserPolicy,
          SQLConf.SESSION_LOCAL_TIMEZONE.key -> JST_OPT.get) {
          checkEvaluation(
            DateFormatClass(Cast(Literal(d), TimestampType, JST_OPT), Literal("y"), JST_OPT),
            "2015")
          checkEvaluation(DateFormatClass(Literal(ts), Literal("y"), JST_OPT), "2013")
          checkEvaluation(
            DateFormatClass(Cast(Literal(d), TimestampType, JST_OPT), Literal("H"), JST_OPT),
            "0")
          checkEvaluation(DateFormatClass(Literal(ts), Literal("H"), JST_OPT), "22")
        }
    }
  }

  testGluten("from_unixtime") {
    val outstandingTimezonesIds: Seq[String] = Seq(
      // Velox doesn't support timezones like "UTC".
      // "UTC",
      // Not supported in velox.
      // PST.getId,
      // CET.getId,
      "Africa/Dakar",
      LA.getId,
      "Asia/Urumqi",
      "Asia/Hong_Kong",
      "Europe/Brussels"
    )
    val outstandingZoneIds: Seq[ZoneId] = outstandingTimezonesIds.map(getZoneId)
    Seq("legacy", "corrected").foreach {
      legacyParserPolicy =>
        for (zid <- outstandingZoneIds) {
          withSQLConf(
            SQLConf.LEGACY_TIME_PARSER_POLICY.key -> legacyParserPolicy,
            SQLConf.SESSION_LOCAL_TIMEZONE.key -> zid.getId) {
            val fmt1 = "yyyy-MM-dd HH:mm:ss"
            val sdf1 = new SimpleDateFormat(fmt1, Locale.US)
            val fmt2 = "yyyy-MM-dd HH:mm:ss.SSS"
            val sdf2 = new SimpleDateFormat(fmt2, Locale.US)
            val timeZoneId = Option(zid.getId)
            val tz = TimeZone.getTimeZone(zid)
            sdf1.setTimeZone(tz)
            sdf2.setTimeZone(tz)

            checkEvaluation(
              FromUnixTime(Literal(0L), Literal(fmt1), timeZoneId),
              sdf1.format(new Timestamp(0)))
            checkEvaluation(
              FromUnixTime(Literal(1000L), Literal(fmt1), timeZoneId),
              sdf1.format(new Timestamp(1000000)))
            checkEvaluation(
              FromUnixTime(Literal(-1000L), Literal(fmt2), timeZoneId),
              sdf2.format(new Timestamp(-1000000)))
            checkEvaluation(
              FromUnixTime(Literal(Long.MaxValue), Literal(fmt2), timeZoneId),
              sdf2.format(new Timestamp(-1000)))
            checkEvaluation(
              FromUnixTime(
                Literal.create(null, LongType),
                Literal.create(null, StringType),
                timeZoneId),
              null)
            checkEvaluation(
              FromUnixTime(Literal.create(null, LongType), Literal(fmt1), timeZoneId),
              null)
            checkEvaluation(
              FromUnixTime(Literal(1000L), Literal.create(null, StringType), timeZoneId),
              null)

            // SPARK-28072 The codegen path for non-literal input should also work
            checkEvaluation(
              expression = FromUnixTime(
                BoundReference(ordinal = 0, dataType = LongType, nullable = true),
                BoundReference(ordinal = 1, dataType = StringType, nullable = true),
                timeZoneId),
              expected = UTF8String.fromString(sdf1.format(new Timestamp(0))),
              inputRow = InternalRow(0L, UTF8String.fromString(fmt1))
            )
          }
        }
    }
    // Test escaping of format
    GenerateUnsafeProjection.generate(FromUnixTime(Literal(0L), Literal("\""), UTC_OPT) :: Nil)
  }

  testGluten("Hour") {
    val outstandingTimezonesIds: Seq[String] = Seq(
      // Velox doesn't support timezones like "UTC".
      // "UTC",
      // Due to known issue: "-08:00/+01:00 not found in timezone database",
      // skip check PST, CET timezone here.
      // https://github.com/facebookincubator/velox/issues/7804
      // PST.getId, CET.getId,
      "Africa/Dakar",
      LA.getId,
      "Asia/Urumqi",
      "Asia/Hong_Kong",
      "Europe/Brussels"
    )
    withDefaultTimeZone(UTC) {
      Seq("legacy", "corrected").foreach {
        legacyParserPolicy =>
          withSQLConf(
            SQLConf.LEGACY_TIME_PARSER_POLICY.key -> legacyParserPolicy
          ) {
            assert(Hour(Literal.create(null, DateType), UTC_OPT).resolved === false)
            assert(Hour(Literal(ts), UTC_OPT).resolved)
            Seq(TimestampType, TimestampNTZType).foreach {
              dt =>
                checkEvaluation(Hour(Cast(Literal(d), dt, UTC_OPT), UTC_OPT), 0)
                checkEvaluation(Hour(Cast(Literal(date), dt, UTC_OPT), UTC_OPT), 13)
            }
            checkEvaluation(Hour(Literal(ts), UTC_OPT), 13)
          }

          val c = Calendar.getInstance()
          outstandingTimezonesIds.foreach {
            zid =>
              withSQLConf(
                SQLConf.LEGACY_TIME_PARSER_POLICY.key -> legacyParserPolicy,
                SQLConf.SESSION_LOCAL_TIMEZONE.key -> zid
              ) {
                val timeZoneId = Option(zid)
                c.setTimeZone(TimeZone.getTimeZone(zid))
                (0 to 24 by 5).foreach {
                  h =>
                    // validate timestamp with local time zone
                    c.set(2015, 18, 3, h, 29, 59)
                    checkEvaluation(
                      Hour(Literal(new Timestamp(c.getTimeInMillis)), timeZoneId),
                      c.get(Calendar.HOUR_OF_DAY))

                    // validate timestamp without time zone
                    val localDateTime = LocalDateTime.of(2015, 1, 3, h, 29, 59)
                    checkEvaluation(Hour(Literal(localDateTime), timeZoneId), h)
                }
                Seq(TimestampType, TimestampNTZType).foreach {
                  dt =>
                    checkConsistencyBetweenInterpretedAndCodegen(
                      (child: Expression) => Hour(child, timeZoneId),
                      dt)
                }
              }
          }
      }
    }
  }

  testGluten("SPARK-42635: timestampadd near daylight saving transition") {
    // In America/Los_Angeles timezone, timestamp value `skippedTime` is 2011-03-13 03:00:00.
    // The next second of 2011-03-13 01:59:59 jumps to 2011-03-13 03:00:00.
    val skippedTime = 1300010400000000L
    // In America/Los_Angeles timezone, both timestamp range `[repeatedTime - MICROS_PER_HOUR,
    // repeatedTime)` and `[repeatedTime, repeatedTime + MICROS_PER_HOUR)` map to
    // [2011-11-06 01:00:00, 2011-11-06 02:00:00).
    // The next second of 2011-11-06 01:59:59 (pre-transition) jumps back to 2011-11-06 01:00:00.
    val repeatedTime = 1320570000000000L
    withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> LA.getId) {
      // Adding one day is **not** equivalent to adding <unit>_PER_DAY time units, because not every
      // day has 24 hours: 2011-03-13 has 23 hours, 2011-11-06 has 25 hours.

      // timestampadd(DAY, 1L, 2011-03-12 03:00:00) = 2011-03-13 03:00:00
      checkEvaluation(
        TimestampAdd(
          "DAY",
          Literal(1L),
          Literal(skippedTime - 23 * MICROS_PER_HOUR, TimestampType)),
        skippedTime)
      // timestampadd(HOUR, 24L, 2011-03-12 03:00:00) = 2011-03-13 04:00:00
      checkEvaluation(
        TimestampAdd(
          "HOUR",
          Literal(24L),
          Literal(skippedTime - 23 * MICROS_PER_HOUR, TimestampType)),
        skippedTime + MICROS_PER_HOUR)
      // timestampadd(HOUR, 23L, 2011-03-12 03:00:00) = 2011-03-13 03:00:00
      checkEvaluation(
        TimestampAdd(
          "HOUR",
          Literal(23L),
          Literal(skippedTime - 23 * MICROS_PER_HOUR, TimestampType)),
        skippedTime)
      // timestampadd(SECOND, SECONDS_PER_DAY, 2011-03-12 03:00:00) = 2011-03-13 04:00:00
      checkEvaluation(
        TimestampAdd(
          "SECOND",
          Literal(SECONDS_PER_DAY),
          Literal(skippedTime - 23 * MICROS_PER_HOUR, TimestampType)),
        skippedTime + MICROS_PER_HOUR)
      // timestampadd(SECOND, SECONDS_PER_DAY, 2011-03-12 03:00:00) = 2011-03-13 03:59:59
      checkEvaluation(
        TimestampAdd(
          "SECOND",
          Literal(SECONDS_PER_DAY - 1),
          Literal(skippedTime - 23 * MICROS_PER_HOUR, TimestampType)),
        skippedTime + MICROS_PER_HOUR - MICROS_PER_SECOND
      )

      // timestampadd(DAY, 1L, 2011-11-05 02:00:00) = 2011-11-06 02:00:00
      checkEvaluation(
        TimestampAdd(
          "DAY",
          Literal(1L),
          Literal(repeatedTime - 24 * MICROS_PER_HOUR, TimestampType)),
        repeatedTime + MICROS_PER_HOUR)
      // timestampadd(DAY, 1L, 2011-11-05 01:00:00) = 2011-11-06 01:00:00 (pre-transition)
      checkEvaluation(
        TimestampAdd(
          "DAY",
          Literal(1L),
          Literal(repeatedTime - 25 * MICROS_PER_HOUR, TimestampType)),
        repeatedTime - MICROS_PER_HOUR)
      // timestampadd(DAY, -1L, 2011-11-07 01:00:00) = 2011-11-06 01:00:00 (post-transition)
      // Vanilla spark result is 1320570000000000L, velox result is 1320566400000000L, they
      // are all 2011-11-06 01:00:00.
      checkEvaluation(
        TimestampAdd(
          "DAY",
          Literal(-1L),
          Literal(repeatedTime + 24 * MICROS_PER_HOUR, TimestampType)),
        repeatedTime - MICROS_PER_HOUR)
      // timestampadd(MONTH, 1L, 2011-10-06 01:00:00) = 2011-11-06 01:00:00 (pre-transition)
      checkEvaluation(
        TimestampAdd(
          "MONTH",
          Literal(1L),
          Literal(repeatedTime - MICROS_PER_HOUR - 31 * MICROS_PER_DAY, TimestampType)),
        repeatedTime - MICROS_PER_HOUR)
      // timestampadd(MONTH, -1L, 2011-12-06 01:00:00) = 2011-11-06 01:00:00 (post-transition)
      // Vanilla spark result is 1320570000000000L, velox result is 1320566400000000L, they
      // are all 2011-11-06 01:00:00.
      checkEvaluation(
        TimestampAdd(
          "MONTH",
          Literal(-1L),
          Literal(repeatedTime + 30 * MICROS_PER_DAY, TimestampType)),
        repeatedTime - MICROS_PER_HOUR)
      // timestampadd(HOUR, 23L, 2011-11-05 02:00:00) = 2011-11-06 01:00:00 (pre-transition)
      checkEvaluation(
        TimestampAdd(
          "HOUR",
          Literal(23L),
          Literal(repeatedTime - 24 * MICROS_PER_HOUR, TimestampType)),
        repeatedTime - MICROS_PER_HOUR)
      // timestampadd(HOUR, 24L, 2011-11-05 02:00:00) = 2011-11-06 01:00:00 (post-transition)
      checkEvaluation(
        TimestampAdd(
          "HOUR",
          Literal(24L),
          Literal(repeatedTime - 24 * MICROS_PER_HOUR, TimestampType)),
        repeatedTime)
    }
  }

  testGluten("months_between") {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US)
    for (zid <- outstandingZoneIds) {
      withSQLConf(
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> zid.getId
      ) {
        val timeZoneId = Option(zid.getId)
        sdf.setTimeZone(TimeZone.getTimeZone(zid))

        checkEvaluation(
          MonthsBetween(
            Literal(new Timestamp(sdf.parse("1997-02-28 10:30:00").getTime)),
            Literal(new Timestamp(sdf.parse("1996-10-30 00:00:00").getTime)),
            Literal.TrueLiteral,
            timeZoneId = timeZoneId
          ),
          3.94959677
        )
        checkEvaluation(
          MonthsBetween(
            Literal(new Timestamp(sdf.parse("1997-02-28 10:30:00").getTime)),
            Literal(new Timestamp(sdf.parse("1996-10-30 00:00:00").getTime)),
            Literal.FalseLiteral,
            timeZoneId = timeZoneId
          ),
          3.9495967741935485
        )

        Seq(Literal.FalseLiteral, Literal.TrueLiteral).foreach {
          roundOff =>
            checkEvaluation(
              MonthsBetween(
                Literal(new Timestamp(sdf.parse("2015-01-30 11:52:00").getTime)),
                Literal(new Timestamp(sdf.parse("2015-01-30 11:50:00").getTime)),
                roundOff,
                timeZoneId = timeZoneId
              ),
              0.0
            )
            checkEvaluation(
              MonthsBetween(
                Literal(new Timestamp(sdf.parse("2015-01-31 00:00:00").getTime)),
                Literal(new Timestamp(sdf.parse("2015-03-31 22:00:00").getTime)),
                roundOff,
                timeZoneId = timeZoneId
              ),
              -2.0
            )
            checkEvaluation(
              MonthsBetween(
                Literal(new Timestamp(sdf.parse("2015-03-31 22:00:00").getTime)),
                Literal(new Timestamp(sdf.parse("2015-02-28 00:00:00").getTime)),
                roundOff,
                timeZoneId = timeZoneId
              ),
              1.0
            )
        }
        val t = Literal(Timestamp.valueOf("2015-03-31 22:00:00"))
        val tnull = Literal.create(null, TimestampType)
        checkEvaluation(MonthsBetween(t, tnull, Literal.TrueLiteral, timeZoneId = timeZoneId), null)
        checkEvaluation(MonthsBetween(tnull, t, Literal.TrueLiteral, timeZoneId = timeZoneId), null)
        checkEvaluation(
          MonthsBetween(tnull, tnull, Literal.TrueLiteral, timeZoneId = timeZoneId),
          null)
        checkEvaluation(
          MonthsBetween(t, t, Literal.create(null, BooleanType), timeZoneId = timeZoneId),
          null)
        checkConsistencyBetweenInterpretedAndCodegen(
          (time1: Expression, time2: Expression, roundOff: Expression) =>
            MonthsBetween(time1, time2, roundOff, timeZoneId = timeZoneId),
          TimestampType,
          TimestampType,
          BooleanType
        )
      }
    }
  }
}

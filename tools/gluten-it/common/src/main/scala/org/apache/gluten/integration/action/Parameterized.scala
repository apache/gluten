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
package org.apache.gluten.integration.action

import org.apache.gluten.integration.{Query, QueryRunner, Suite}
import org.apache.gluten.integration.QueryRunner.QueryResult
import org.apache.gluten.integration.action.Actions.QuerySelector
import org.apache.gluten.integration.action.TableRender.Field
import org.apache.gluten.integration.action.TableRender.RowParser.FieldAppender.RowAppender
import org.apache.gluten.integration.stat.RamStat

import org.apache.spark.sql.ConfUtils.ConfImplicits._
import org.apache.spark.sql.SparkSession

import java.io.PrintStream
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import scala.collection.mutable

class Parameterized(
    queries: QuerySelector,
    explain: Boolean,
    iterations: Int,
    warmupIterations: Int,
    noSessionReuse: Boolean,
    configDimensions: Seq[Parameterized.Dim],
    excludedCombinations: Seq[Set[Parameterized.DimKv]],
    metrics: Seq[String])
  extends Action {
  import Parameterized._

  validateDims(configDimensions)

  private def validateDims(configDimensions: Seq[Dim]): Unit = {
    if (
      configDimensions
        .map(
          dim => {
            dim.name
          })
        .toSet
        .size != configDimensions.size
    ) {
      throw new IllegalStateException("Duplicated dimension name found")
    }

    configDimensions.foreach {
      dim =>
        if (dim.dimValues.map(dimValue => dimValue.name).toSet.size != dim.dimValues.size) {
          throw new IllegalStateException("Duplicated dimension value found")
        }
    }
  }

  private val coordinates: mutable.LinkedHashMap[Coordinate, Seq[(String, String)]] = {
    val dimCount = configDimensions.size
    val coordinateMap = mutable.LinkedHashMap[Coordinate, Seq[(String, String)]]()
    val nextId: AtomicInteger = new AtomicInteger(1);

    def fillCoordinates(
        dimOffset: Int,
        intermediateCoordinate: Map[String, String],
        intermediateConf: Seq[(String, String)]): Unit = {
      if (dimOffset == dimCount) {
        // we got one coordinate
        excludedCombinations.foreach {
          ec: Set[DimKv] =>
            if (
              ec.forall {
                kv => intermediateCoordinate.contains(kv.k) && intermediateCoordinate(kv.k) == kv.v
              }
            ) {
              println(s"Coordinate $intermediateCoordinate excluded by $ec.")
              return
            }
        }
        coordinateMap(Coordinate(nextId.getAndIncrement(), intermediateCoordinate)) =
          intermediateConf
        return
      }
      val dim = configDimensions(dimOffset)
      dim.dimValues.foreach {
        dimValue =>
          fillCoordinates(
            dimOffset + 1,
            intermediateCoordinate + (dim.name -> dimValue.name),
            intermediateConf ++ dimValue.conf)
      }
    }

    fillCoordinates(0, Map(), Seq())

    coordinateMap
  }

  override def execute(suite: Suite): Boolean = {
    val runner: QueryRunner =
      new QueryRunner(suite.dataSource(), suite.dataWritePath())

    val sessionSwitcher = suite.sessionSwitcher
    val testConf = suite.getTestConf()

    println("Prepared coordinates: ")
    coordinates.keys.foreach(c => println(s"  ${c.id}: $c"))
    coordinates.foreach {
      entry =>
        // register one session per coordinate
        val coordinate = entry._1
        val coordinateConf = entry._2
        val conf = testConf.clone()
        conf.setAllWarningOnOverriding(coordinateConf)
        sessionSwitcher.registerSession(coordinate.toString, conf)
    }

    val querySet = queries.select(suite)
    val runQueryIds = querySet.queryIds.map(TestResultLine.QueryId(_))

    val marks: Seq[TestResultLine.CoordMark] = coordinates.flatMap {
      entry =>
        val coordinate = entry._1
        sessionSwitcher.useSession(coordinate.toString, "Parameterized %s".format(coordinate))
        runner.createTables(suite.tableCreator(), suite.tableAnalyzer(), sessionSwitcher.spark())

        runQueryIds.flatMap {
          queryId =>
            // warm up
            (0 until warmupIterations).foreach {
              iteration =>
                println(s"Warming up: Running query $queryId (iteration $iteration)...")
                try {
                  Parameterized.warmUp(
                    runner,
                    sessionSwitcher.spark(),
                    querySet.getQuery(queryId.id),
                    coordinate,
                    suite.desc())
                } finally {
                  if (noSessionReuse) {
                    sessionSwitcher.renewSession()
                    runner.createTables(
                      suite.tableCreator(),
                      suite.tableAnalyzer(),
                      sessionSwitcher.spark())
                  }
                }
            }

            // run
            (0 until iterations).map {
              iteration =>
                println(
                  s"Running query $queryId with coordinate $coordinate (iteration $iteration)...")
                val r =
                  try {
                    Parameterized.runQuery(
                      runner,
                      sessionSwitcher.spark(),
                      querySet.getQuery(queryId.id),
                      coordinate,
                      suite.desc(),
                      explain,
                      metrics)
                  } finally {
                    if (noSessionReuse) {
                      sessionSwitcher.renewSession()
                      runner.createTables(
                        suite.tableCreator(),
                        suite.tableAnalyzer(),
                        sessionSwitcher.spark())
                    }
                  }
                TestResultLine.CoordMark(iteration, queryId, r)
            }
        }
    }.toSeq

    val results: Seq[TestResultLine] = marks
      .groupBy(m => (m.iteration, m.queryId))
      .toSeq
      .sortBy(_._1)
      .map {
        e =>
          val iteration = e._1._1
          val queryId = e._1._2
          val marks = e._2
          val line = TestResultLine(queryId, marks.map(_.coord).toList)
          line
      }

    val succeededCount = results.count(l => l.succeeded())
    val totalCount = results.count(_ => true)

    // RAM stats
    println("Performing GC to collect RAM statistics... ")
    System.gc()
    System.gc()
    printf(
      "RAM statistics: JVM Heap size: %d KiB (total %d KiB), Process RSS: %d KiB\n",
      RamStat.getJvmHeapUsed(),
      RamStat.getJvmHeapTotal(),
      RamStat.getProcessRamUsed()
    )
    println()

    // Write out test report.
    val reportAppender = suite.getReporter().actionAppender(getClass.getSimpleName)
    reportAppender.out.println("Test report: ")
    reportAppender.out.println()
    reportAppender.out.println(
      "Summary: %d out of %d queries successfully run on all config combinations."
        .format(succeededCount, totalCount))
    reportAppender.out.println()
    reportAppender.out.println("Configurations:")
    coordinates.foreach(coord => reportAppender.out.println(s"${coord._1.id}. ${coord._1}"))
    reportAppender.out.println()
    val succeeded = results.filter(_.succeeded())
    val all = succeeded match {
      case Nil => None
      case several =>
        Some(
          TestResultLine(
            TestResultLine.QueryId("all"),
            coordinates.keys.map {
              c =>
                TestResultLine.Coord(
                  c,
                  several
                    .map(_.coord(c.id))
                    .map(_.queryResult)
                    .asSuccesses()
                    .agg(s"coordinate $c")
                    .get)
            }.toSeq
          ))
    }
    TestResultLines(coordinates.map(_._1.id).toSeq, configDimensions, metrics, succeeded ++ all)
      .print(reportAppender.out)
    reportAppender.out.println()

    if (succeededCount == totalCount) {
      reportAppender.out.println("No failed queries. ")
      reportAppender.out.println()
    } else {
      reportAppender.err.println("Failed queries: ")
      reportAppender.err.println()
      TestResultLines(
        coordinates.map(_._1.id).toSeq,
        configDimensions,
        metrics,
        results.filter(!_.succeeded()))
        .print(reportAppender.err)
      reportAppender.err.println()
    }

    if (succeededCount != totalCount) {
      return false
    }
    true
  }
}

object Parameterized {
  case class DimKv(k: String, v: String)

  case class Dim(name: String, dimValues: Seq[DimValue])

  case class DimValue(name: String, conf: Seq[(String, String)])

  // coordinate: [dim, dim value]
  case class Coordinate(id: Int, coordinate: Map[String, String]) {
    override def toString: String = coordinate.mkString(", ")
  }

  case class TestResultLine(
      queryId: TestResultLine.QueryId,
      coordinates: Seq[TestResultLine.Coord]) {
    private val coordMap = coordinates.map(c => c.coordinate.id -> c).toMap
    def succeeded(): Boolean = {
      coordinates.forall(_.queryResult.succeeded())
    }

    def coord(id: Int): TestResultLine.Coord = coordMap(id)
  }

  object TestResultLine {
    case class QueryId(id: String) {
      import QueryId._
      private val uid = nextUid.getAndIncrement()
      override def toString: String = id
    }

    object QueryId {
      private val nextUid = new AtomicLong(0L)
      implicit val o: Ordering[QueryId] = Ordering.by(_.uid)
    }

    case class Coord(coordinate: Coordinate, queryResult: QueryResult)
    case class CoordMark(iteration: Int, queryId: QueryId, coord: Coord)

    class Parser(coordIds: Seq[Int], metricNames: Seq[String])
      extends TableRender.RowParser[TestResultLine] {
      override def parse(rowAppender: RowAppender, line: TestResultLine): Unit = {
        val inc = rowAppender.incremental()
        inc.next().write(line.queryId)
        val coords = coordIds.map(id => line.coord(id))
        coords.foreach(coord => inc.next().write(coord.queryResult.succeeded()))
        coords.foreach(
          coord => inc.next().write(coord.queryResult.asSuccessOption().map(_.runResult.rows.size)))
        metricNames.foreach(
          metricName =>
            coords.foreach(
              coord =>
                inc
                  .next()
                  .write(
                    coord.queryResult
                      .asSuccessOption()
                      .map(_.runResult.executorMetrics(metricName)))))
        coords.foreach(
          coord =>
            inc
              .next()
              .write(coord.queryResult.asSuccessOption().map(_.runResult.planningTimeMillis)))
        coords.foreach(
          coord =>
            inc
              .next()
              .write(coord.queryResult.asSuccessOption().map(_.runResult.executionTimeMillis)))
      }
    }
  }

  case class TestResultLines(
      coordIds: Seq[Int],
      configDimensions: Seq[Dim],
      metricNames: Seq[String],
      lines: Iterable[TestResultLine]) {
    def print(out: PrintStream): Unit = {
      val coordFields: Seq[Field] = coordIds.map(id => Field.Leaf(id.toString))
      val fields: Seq[Field] =
        Seq(Field.Leaf("Query ID")) ++
          Seq(Field.Branch("Succeeded", coordFields), Field.Branch("Row Count", coordFields)) ++
          metricNames.map(metricName => Field.Branch(metricName, coordFields)) ++
          Seq(
            Field.Branch("Planning Time (Millis)", coordFields),
            Field.Branch("Query Time (Millis)", coordFields)
          )
      val render =
        TableRender.create[TestResultLine](fields: _*)(
          new TestResultLine.Parser(coordIds, metricNames)
        )
      lines.foreach(line => render.appendRow(line))

      render.print(out)
    }
  }

  private def runQuery(
      runner: QueryRunner,
      spark: SparkSession,
      query: Query,
      coordinate: Coordinate,
      desc: String,
      explain: Boolean,
      metrics: Seq[String]): TestResultLine.Coord = {
    val testDesc = "Query %s [%s] %s".format(desc, query.id, coordinate)
    val result = runner.runQuery(spark, testDesc, query, explain, executorMetrics = metrics)
    TestResultLine.Coord(coordinate, result)
  }

  private def warmUp(
      runner: QueryRunner,
      session: SparkSession,
      query: Query,
      coordinate: Coordinate,
      desc: String): Unit = {
    runQuery(runner, session, query, coordinate, desc, explain = false, Nil)
  }
}

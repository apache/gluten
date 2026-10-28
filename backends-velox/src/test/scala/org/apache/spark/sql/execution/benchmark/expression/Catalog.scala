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
package org.apache.spark.sql.execution.benchmark.expression

import org.apache.gluten.utils.Arm

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.vladsch.flexmark.ast.{Code, Heading, Paragraph, SoftLineBreak, Text}
import com.vladsch.flexmark.ext.tables.{TableBlock, TableCell, TableHead, TableRow, TableSeparator, TablesExtension}
import com.vladsch.flexmark.parser.Parser
import com.vladsch.flexmark.util.ast.Node
import com.vladsch.flexmark.util.data.MutableDataSet
import com.vladsch.flexmark.util.misc.Extension
import org.apache.commons.io.IOUtils

import java.io.FileNotFoundException
import java.lang.{Boolean => JBoolean}
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8
import java.util.Collections

import scala.collection.JavaConverters._
import scala.collection.mutable.HashSet
import scala.util.control.NonFatal
import scala.util.parsing.combinator.JavaTokenParsers

/** Classpath-backed test data, deliberately independent of selection and benchmark execution. */
private[benchmark] object Catalog {
  final case class SourceLocation(file: String, line: Int, column: Int) {
    override def toString: String = s"$file:$line:$column"
  }
  sealed trait Argument
  final case class StringArgument(value: String) extends Argument
  final case class LongArgument(value: Long) extends Argument
  final case class BooleanArgument(value: Boolean) extends Argument
  final case class Binding(
      names: Seq[String],
      generator: String,
      arguments: Seq[(String, Argument)])
  final case class CaseDef(
      id: String,
      inputs: Seq[Binding],
      sql: String,
      description: String,
      sourceLocation: SourceLocation,
      groupingKeys: Option[Seq[String]] = None,
      mode: Option[String] = None,
      maps: Option[Int] = None,
      reducers: Option[Int] = None,
      keyCardinality: Option[Long] = None,
      rows: Option[Int] = None)

  private val markdown = Parser
    .builder(
      new MutableDataSet()
        .set(Parser.EXTENSIONS, Collections.singletonList[Extension](TablesExtension.create()))
        .set(TablesExtension.COLUMN_SPANS, JBoolean.FALSE)
        .set(TablesExtension.APPEND_MISSING_COLUMNS, JBoolean.FALSE)
        .set(TablesExtension.DISCARD_EXTRA_COLUMNS, JBoolean.FALSE)
        .set(TablesExtension.HEADER_SEPARATOR_COLUMN_MATCH, JBoolean.TRUE))
    .build()

  private def fail(location: SourceLocation, caseId: String, cause: Throwable): Nothing =
    throw new IllegalArgumentException(s"$location case=$caseId: ${cause.getMessage}", cause)

  private def checked[T](location: SourceLocation, caseId: String)(f: => T): T =
    try f
    catch { case NonFatal(cause) => fail(location, caseId, cause) }

  def parseMarkdown(
      function: String,
      file: String,
      text: String,
      aggregate: Boolean = false): Seq[CaseDef] = {
    def location(node: Node): SourceLocation = {
      val offset = node.getStartOffset
      SourceLocation(file, node.getStartLineNumber + 1, offset - text.lastIndexOf('\n', offset - 1))
    }
    def check(condition: Boolean, node: Node, id: String, message: String): Unit =
      if (!condition) fail(location(node), id, new IllegalArgumentException(message))

    checked(SourceLocation(file, 1, 1), function) {
      require(function.matches("[a-z0-9_-]+"), s"Invalid function ID: $function")
    }
    val document = checked(SourceLocation(file, 1, 1), function)(markdown.parse(text))
    val tables = document.getDescendants.asScala.collect { case t: TableBlock => t }.toVector
    check(tables.size == 1, document, function, "Expected exactly one case table")
    val table = tables.head
    document.getChildren.asScala.foreach {
      node =>
        val explanation = (node.isInstanceOf[Paragraph] || node.isInstanceOf[Heading]) &&
          node.getChildren.asScala.forall {
            case _: Text | _: SoftLineBreak => true
            case _ => false
          } && !node.getChars.toString.contains('|')
        // A broken table row must not be accepted as an ordinary explanation paragraph.
        check(
          node == table || explanation,
          node,
          function,
          "Only plain headings, explanation paragraphs and the case table are allowed")
    }
    def plain(cell: TableCell, id: String): String = {
      check(
        cell.getChildren.asScala.forall(_.isInstanceOf[Text]),
        cell,
        id,
        "Expected plain text, without HTML, links or mixed markup")
      val value = cell.getText.toString.trim
      check(
        value.nonEmpty && !value.contains('\n') && !value.contains('\r'),
        cell,
        id,
        "Expected a nonempty single-line cell")
      value
    }
    def code(cell: TableCell, id: String): (String, SourceLocation, Vector[Int]) = {
      val children = cell.getChildren.asScala.filterNot {
        case text: Text => text.getChars.toString.trim.isEmpty
        case _ => false
      }.toVector
      check(
        children.size == 1 && children.head.isInstanceOf[Code],
        cell,
        id,
        "Expected one complete code span")
      val span = children.head.asInstanceOf[Code]
      val raw = span.getText.toString
      check(
        raw.trim.nonEmpty && !raw.contains('\n') && !raw.contains('\r'),
        cell,
        id,
        "Expected a nonempty single-line code span")
      // Keep UTF-16 source offsets while removing only the table-layer escape before a pipe.
      val offsets =
        raw.indices.filterNot(i => raw(i) == '\\' && raw.lift(i + 1).contains('|')).toVector
      (
        offsets.map(raw.charAt).mkString,
        location(span).copy(column = location(span).column + span.getOpeningMarker.length()),
        offsets :+ raw.length)
    }
    val rows = table.getDescendants.asScala.collect {
      case row: TableRow if !row.getParent.isInstanceOf[TableSeparator] => row
    }.toVector
    check(
      rows.nonEmpty && rows.head.getParent.isInstanceOf[TableHead],
      table,
      function,
      "Missing table header")
    val required = Seq("case", "inputs", "expression", "description") ++
      (if (aggregate) Seq("grouping") else Seq.empty)
    val optional = Set("key-cardinality", "rows") ++
      (if (aggregate) Set("mode", "maps", "reducers") else Set.empty[String])
    val headers = rows.head.getChildren.asScala.collect {
      case cell: TableCell => plain(cell, function)
    }.toVector
    check(
      headers.startsWith(required) && headers.distinct.size == headers.size &&
        headers.drop(required.size).forall(optional),
      rows.head,
      function,
      s"Expected columns ${required.mkString(", ")}, optionally ${optional.mkString(", ")}"
    )
    val seen = HashSet.empty[String]
    rows.tail.map {
      row =>
        val cells = row.getChildren.asScala.collect { case cell: TableCell => cell }.toVector
        val hint = cells.headOption.map(_.getText.toString.trim).getOrElse("?")
        val id = s"$function/$hint"
        check(
          cells.size == headers.size,
          row,
          id,
          s"Expected ${headers.size} columns, found ${cells.size}")
        val localId = plain(cells(0), id)
        check(localId.matches("[a-z0-9_-]+"), cells(0), id, "Invalid local case ID")
        check(seen.add(localId), cells(0), id, "Duplicate case ID")
        val (input, inputLocation, inputOffsets) = code(cells(1), id)
        val (sql, _, _) = code(cells(2), id)
        def parameter[T](name: String)(parse: String => T): Option[T] = {
          val column = headers.indexOf(name)
          if (column < 0 || cells(column).getText.toString.trim == "-") None
          else Some(checked(location(cells(column)), id)(parse(plain(cells(column), id))))
        }
        def positiveInt(value: String): Int = {
          val number = value.toInt
          require(number > 0, "Expected a positive integer")
          number
        }
        CaseDef(
          id,
          parseInputs(input, inputLocation, id, inputOffsets),
          sql,
          plain(cells(3), id),
          location(cells(0)),
          if (aggregate) {
            Some(
              if (cells(4).getText.toString.trim == "-") Seq.empty
              else Seq(code(cells(4), id)._1))
          } else {
            None
          },
          mode = parameter("mode") {
            value =>
              require(Set("complete", "partial-final")(value), s"Unknown aggregate mode: $value")
              value
          },
          rows = parameter("rows")(positiveInt),
          maps = parameter("maps")(positiveInt),
          reducers = parameter("reducers")(positiveInt),
          keyCardinality = parameter("key-cardinality") {
            value =>
              val number = value.toLong
              require(number > 0, "Key cardinality must be positive")
              number
          }
        )
    } match {
      case cases if cases.nonEmpty => cases
      case _ => fail(location(table), function, new IllegalArgumentException("Empty case table"))
    }
  }

  private object InputsParser extends JavaTokenParsers {
    private val json = new ObjectMapper()
      .readerFor(classOf[String])
      .`with`(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
    private def identifier: Parser[String] = "[A-Za-z_][A-Za-z0-9_]*".r
    private def registry: Parser[String] = rep1sep(identifier, ".") ^^ (_.mkString("."))
    private def value: Parser[Argument] =
      ("\"(?:[^\"\\\\\r\n]|\\\\.)*\"".r ^^ (s => StringArgument(json.readValue[String](s)))) |
        ("-?(?:0|[1-9][0-9]*)".r ^^ (s => LongArgument(s.toLong))) |
        ("true" ^^^ BooleanArgument(true)) | ("false" ^^^ BooleanArgument(false))
    private def argument: Parser[(String, Argument)] =
      identifier ~ ("=" ~> value) ^^ { case name ~ v => name -> v }
    private def names: Parser[Seq[String]] =
      ("(" ~> identifier ~ ("," ~> rep1sep(identifier, ",")) <~ ")" ^^ {
        case first ~ rest => first +: rest
      }) | (identifier ^^ (Seq(_)))
    private def binding: Parser[Binding] =
      names ~ ("=" ~> registry) ~ ("(" ~> repsep(argument, ",") <~ ")") ^^ {
        case columns ~ name ~ args => Binding(columns, name, args)
      }
    def bindings: Parser[Seq[Binding]] =
      rep1sep(binding, ";") | ("none" ^^^ Seq.empty[Binding])
  }

  def parseInputs(text: String, location: SourceLocation, caseId: String): Seq[Binding] =
    parseInputs(text, location, caseId, Vector.empty)

  private def parseInputs(
      text: String,
      location: SourceLocation,
      caseId: String,
      sourceOffsets: Vector[Int]): Seq[Binding] = {
    val parsed: InputsParser.ParseResult[Seq[Binding]] =
      checked(location, caseId)(InputsParser.parseAll(InputsParser.bindings, text))
    val bindings: Seq[Binding] = parsed match {
      case InputsParser.Success(bindings, _) => bindings
      case error: InputsParser.NoSuccess =>
        val pos = error.next.pos
        val physical = SourceLocation(
          location.file,
          location.line + pos.line - 1,
          if (pos.line == 1) {
            location.column + sourceOffsets.lift(pos.column - 1).getOrElse(pos.column - 1)
          } else {
            pos.column
          })
        fail(physical, caseId, new IllegalArgumentException(s"Invalid inputs: ${error.msg}"))
    }
    checked(location, caseId)(Data.validate(bindings))
    bindings
  }

  def parseIndex(text: String, file: String): Seq[String] = {
    val entries = text.linesIterator.toVector
    if (entries.isEmpty) {
      fail(SourceLocation(file, 1, 1), "index", new IllegalArgumentException("Empty index"))
    }
    val seen = HashSet.empty[String]
    entries.zipWithIndex.foreach {
      case (entry, i) =>
        checked(SourceLocation(file, i + 1, 1), "index") {
          require(entry.matches("[a-z0-9_-]+\\.md"), s"Invalid filename: $entry")
          require(seen.add(entry), s"Duplicate index entry: $entry")
        }
    }
    entries
  }

  def loadAggregates(): Seq[CaseDef] =
    load(root = "expression-benchmark/aggregate", aggregate = true)

  def load(
      loader: ClassLoader = getClass.getClassLoader,
      root: String = "expression-benchmark/cases",
      aggregate: Boolean = false): Seq[CaseDef] = {
    def read(file: String): String = checked(SourceLocation(file, 1, 1), "index") {
      Arm.withResource(
        Option(loader.getResourceAsStream(file))
          .getOrElse(throw new FileNotFoundException(file))) {
        input =>
          // A decoder reports malformed UTF-8 rather than replacing bytes in SQL or IDs.
          UTF_8.newDecoder().decode(ByteBuffer.wrap(IOUtils.toByteArray(input))).toString
      }
    }
    val index = s"$root/index.txt"
    parseIndex(read(index), index).flatMap {
      file =>
        parseMarkdown(
          file.stripSuffix(".md"),
          s"$root/$file",
          read(s"$root/$file"),
          aggregate)
    }
  }
}

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

import org.apache.gluten.config.GlutenConfig

import com.google.protobuf.Descriptors.{Descriptor, FieldDescriptor}
import io.substrait.proto.ReadRel
import org.scalatest.funsuite.AnyFunSuite

import java.util.{Collections, Map => JMap}

/**
 * Pins the wire tags of the vendored `DelimiterSeparatedTextReadOptions`. Producer and consumer
 * share one schema, so a renumber or a field rename round-trips cleanly through the generated
 * classes and cannot be caught by exercising them; these assert on the descriptors instead.
 *
 * The critical guard is that Gluten's `max_block_size` (rows per output block) and the standard
 * `max_line_size` (bytes per line) stay two distinct fields: `max_block_size` lives in the 1000
 * graft range, well clear of `max_line_size` on tag 2. Wiring the row count onto tag 2 would
 * compile but silently change its meaning.
 *
 * The producer tests at the end cover what descriptors cannot: `value_treated_as_null` is
 * `optional`, so setting it -- even to the empty string -- declares "this value is NULL" and makes
 * the whole file nullable strings. `LocalFilesNode` must therefore leave it unset when the reader
 * supplied no `nullValue`.
 */
class DelimiterSeparatedTextReadOptionsProtoSuite extends AnyFunSuite {

  private def fileOrFiles: Descriptor =
    ReadRel.LocalFiles.FileOrFiles.getDescriptor

  private def textOptions: Descriptor =
    ReadRel.LocalFiles.FileOrFiles.DelimiterSeparatedTextReadOptions.getDescriptor

  private def field(name: String, descriptor: Descriptor): FieldDescriptor = {
    val f = descriptor.findFieldByName(name)
    assert(f != null, s"${descriptor.getName} has no field named $name")
    f
  }

  /** Builds a text split through the real producer so the emitted options can be inspected. */
  private def producedTextOptions(
      properties: JMap[String, String]
  ): ReadRel.LocalFiles.FileOrFiles.DelimiterSeparatedTextReadOptions = {
    val node = LocalFilesBuilder.makeLocalFiles(
      Integer.valueOf(0),
      Collections.singletonList("file:///tmp/students.csv"),
      Collections.singletonList(java.lang.Long.valueOf(0L)),
      Collections.singletonList(java.lang.Long.valueOf(56L)),
      Collections.emptyList[java.lang.Long](),
      Collections.emptyList[java.lang.Long](),
      Collections.singletonList[JMap[String, String]](Collections.emptyMap[String, String]()),
      Collections.emptyList[JMap[String, String]](),
      LocalFilesNode.ReadFileFormat.TextReadFormat,
      Collections.emptyList[String](),
      properties,
      Collections.emptyList[JMap[String, Object]]()
    )
    node.toProtobuf.getItems(0).getText
  }

  test("the message was renamed to DelimiterSeparatedTextReadOptions") {
    assert(
      fileOrFiles.findNestedTypeByName("DelimiterSeparatedTextReadOptions") != null,
      "FileOrFiles must declare DelimiterSeparatedTextReadOptions")
    assert(
      fileOrFiles.findNestedTypeByName("TextReadOptions") === null,
      "the old TextReadOptions message name must be gone")
  }

  test("the standard read-option fields carry their expected tags") {
    assert(field("field_delimiter", textOptions).getNumber === 1)
    assert(field("max_line_size", textOptions).getNumber === 2)
    assert(field("quote", textOptions).getNumber === 3)
    assert(field("header_lines_to_skip", textOptions).getNumber === 4)
    assert(field("escape", textOptions).getNumber === 5)
    assert(field("value_treated_as_null", textOptions).getNumber === 6)
    // value_treated_as_null is `optional`, so it tracks presence.
    assert(
      field("value_treated_as_null", textOptions).hasPresence,
      "value_treated_as_null must stay an optional (presence-tracking) field")
  }

  test("Gluten-local knobs live in the 1000 graft range, distinct from max_line_size") {
    // The trap: max_block_size (rows per block) must not sit on tag 2, which belongs to
    // max_line_size (bytes per line). Conflating them compiles but silently changes meaning.
    assert(
      field("max_block_size", textOptions).getNumber === 1000,
      "max_block_size (rows per block) must be grafted at 1000, NOT conflated with max_line_size")
    assert(field("empty_as_default", textOptions).getNumber === 1001)
  }

  test("the renamed and dropped fork field names are gone") {
    assert(
      textOptions.findFieldByName("header") === null,
      "header was renamed to header_lines_to_skip")
    assert(
      textOptions.findFieldByName("null_value") === null,
      "null_value was renamed to value_treated_as_null")
    assert(
      textOptions.findFieldByName("schema") === null,
      "the deprecated schema field was dropped")
  }

  test("the file_format oneof still exposes text on tag 14 with the renamed message type") {
    val text = field("text", fileOrFiles)
    assert(text.getNumber === 14, "FileOrFiles.text changed its number")
    assert(
      text.getContainingOneof != null && text.getContainingOneof.getName === "file_format",
      "FileOrFiles.text must stay in the file_format oneof")
    assert(
      text.getMessageType.getName === "DelimiterSeparatedTextReadOptions",
      "FileOrFiles.text must hold DelimiterSeparatedTextReadOptions")
  }

  test("LocalFilesNode maps the CSV read properties onto the renamed fields") {
    val properties = new java.util.HashMap[String, String]()
    properties.put("field_delimiter", ",")
    properties.put("quote", "\"")
    properties.put("header", "2")
    properties.put("escape", "\\")
    val opts = producedTextOptions(properties)

    assert(opts.getFieldDelimiter === ",")
    assert(opts.getQuote === "\"")
    assert(opts.getHeaderLinesToSkip === 2L)
    assert(opts.getEscape === "\\")
    // Gluten always drives this from its own config, never from the read properties. Compare
    // against the config rather than a fixed value so the assertion cannot depend on whatever
    // SQLConf happens to be active when the suite runs.
    assert(
      opts.getMaxBlockSize === GlutenConfig.get.textInputMaxBlockSize,
      "max_block_size must come from GlutenConfig")
    assert(
      !opts.getEmptyAsDefault,
      "empty_as_default must come from GlutenConfig, whose default is false")
    // The mirror image, and the regression this suite exists to prevent: the row count must never
    // be wired into the byte-oriented max_line_size on tag 2. Gluten produces no value for it at
    // all.
    assert(opts.getMaxLineSize === 0L, "Gluten must leave max_line_size unset")
  }

  test("LocalFilesNode leaves value_treated_as_null unset when no nullValue was supplied") {
    val opts = producedTextOptions(new java.util.HashMap[String, String]())
    // Setting the field -- even to "" -- means "this value is NULL and the file is entirely
    // nullable strings" (see the field comment in algebra.proto). Absent must stay absent.
    assert(
      !opts.hasValueTreatedAsNull,
      "value_treated_as_null must stay absent when the reader gave no nullValue")
  }

  test("LocalFilesNode sets value_treated_as_null when a nullValue was supplied") {
    val properties = new java.util.HashMap[String, String]()
    properties.put("nullValue", "NULL")
    val opts = producedTextOptions(properties)

    assert(opts.hasValueTreatedAsNull)
    assert(opts.getValueTreatedAsNull === "NULL")
  }
}

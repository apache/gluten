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
package org.apache.gluten.substrait.rel;

import org.apache.gluten.config.GlutenConfig;
import org.apache.gluten.expression.ConverterUtils;
import org.apache.gluten.utils.SubstraitUtil;

import io.substrait.proto.NamedStruct;
import io.substrait.proto.ReadRel;
import io.substrait.proto.Type;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LocalFilesNode implements SplitInfo {
  private final Integer index;
  private final List<String> paths = new ArrayList<>();
  private final List<Long> starts = new ArrayList<>();
  private final List<Long> lengths = new ArrayList<>();
  private final List<Long> fileSizes = new ArrayList<>();
  private final List<Long> modificationTimes = new ArrayList<>();
  private final List<Map<String, String>> partitionColumns = new ArrayList<>();
  private final List<Map<String, String>> metadataColumns = new ArrayList<>();
  private final List<Map<String, Object>> otherMetadataColumns = new ArrayList<>();
  private final List<String> preferredLocations = new ArrayList<>();

  // The format of file to read.
  public enum ReadFileFormat {
    ParquetReadFormat(),
    ArrowReadFormat(),
    OrcReadFormat(),
    DwrfReadFormat(),
    MergeTreeReadFormat(),
    TextReadFormat(),
    JsonReadFormat(),
    KafkaReadFormat(),
    UnknownFormat()
  }

  protected ReadFileFormat fileFormat = ReadFileFormat.UnknownFormat;
  private Boolean iterAsInput = false;
  private StructType fileSchema;
  private Map<String, String> fileReadProperties;

  LocalFilesNode(
      Integer index,
      List<String> paths,
      List<Long> starts,
      List<Long> lengths,
      List<Long> fileSizes,
      List<Long> modificationTimes,
      List<Map<String, String>> partitionColumns,
      List<Map<String, String>> metadataColumns,
      ReadFileFormat fileFormat,
      List<String> preferredLocations,
      Map<String, String> properties,
      List<Map<String, Object>> otherMetadataColumns) {
    this.index = index;
    this.paths.addAll(paths);
    this.starts.addAll(starts);
    this.lengths.addAll(lengths);
    this.fileSizes.addAll(fileSizes);
    this.modificationTimes.addAll(modificationTimes);
    this.fileFormat = fileFormat;
    this.partitionColumns.addAll(partitionColumns);
    this.metadataColumns.addAll(metadataColumns);
    this.preferredLocations.addAll(preferredLocations);
    this.fileReadProperties = properties;
    this.otherMetadataColumns.addAll(otherMetadataColumns);
  }

  LocalFilesNode(String iterPath) {
    this.index = null;
    this.paths.add(iterPath);
    this.iterAsInput = true;
  }

  /**
   * Copies an existing node, replacing its per-file extra metadata. Lets data-lake subclasses
   * decorate a generically built node without re-deriving the file listing.
   */
  protected LocalFilesNode(LocalFilesNode other, List<Map<String, Object>> otherMetadataColumns) {
    this.index = other.index;
    this.paths.addAll(other.paths);
    this.starts.addAll(other.starts);
    this.lengths.addAll(other.lengths);
    this.fileSizes.addAll(other.fileSizes);
    this.modificationTimes.addAll(other.modificationTimes);
    this.partitionColumns.addAll(other.partitionColumns);
    this.metadataColumns.addAll(other.metadataColumns);
    this.fileFormat = other.fileFormat;
    this.preferredLocations.addAll(other.preferredLocations);
    this.fileReadProperties = other.fileReadProperties;
    this.iterAsInput = other.iterAsInput;
    this.fileSchema = other.fileSchema;
    this.otherMetadataColumns.addAll(otherMetadataColumns);
  }

  public List<String> getPaths() {
    return paths;
  }

  public void setPaths(List<String> newPaths) {
    paths.clear();
    paths.addAll(newPaths);
  }

  public void setFileSchema(StructType schema) {
    this.fileSchema = schema;
  }

  private NamedStruct buildNamedStruct() {
    NamedStruct.Builder namedStructBuilder = NamedStruct.newBuilder();

    Type.Struct.Builder structBuilder = Type.Struct.newBuilder();
    for (StructField field : fileSchema.fields()) {
      // A column that contains a VariantType cannot be encoded in a Substrait NamedStruct
      // because ConverterUtils#getTypeNode has no representation for it. If such a column
      // is actually projected from this scan, validation in VeloxBackend rejects the plan and
      // the whole stage falls back to vanilla Spark; we never reach this code path. If the
      // column is *not* projected, however, it still ends up here because the file schema
      // contains every data column of the relation regardless of projection. Skipping the
      // field is safe: the native reader looks up requested columns by name, so an entry
      // that is not requested is unused, and an entry that is missing from the file schema
      // simply means the native reader will not see/validate that column.
      if (containsUnsupportedFileSchemaType(field.dataType())) {
        continue;
      }
      structBuilder.addTypes(
          ConverterUtils.getTypeNode(field.dataType(), field.nullable()).toProtobuf());
      namedStructBuilder.addNames(ConverterUtils.normalizeColName(field.name()));
    }
    namedStructBuilder.setStruct(structBuilder.build());

    return namedStructBuilder.build();
  }

  // Returns true if `dt` is, or recursively contains, a Spark type that has no Substrait
  // representation in ConverterUtils#getTypeNode. Today this is only Spark 4.0's VariantType,
  // matched by name to remain source-compatible with shims that target older Spark versions.
  // Package-private for unit tests.
  static boolean containsUnsupportedFileSchemaType(DataType dt) {
    if ("variant".equals(dt.typeName())) {
      return true;
    }
    if (dt instanceof StructType) {
      for (StructField f : ((StructType) dt).fields()) {
        if (containsUnsupportedFileSchemaType(f.dataType())) {
          return true;
        }
      }
      return false;
    }
    if (dt instanceof ArrayType) {
      return containsUnsupportedFileSchemaType(((ArrayType) dt).elementType());
    }
    if (dt instanceof MapType) {
      MapType m = (MapType) dt;
      return containsUnsupportedFileSchemaType(m.keyType())
          || containsUnsupportedFileSchemaType(m.valueType());
    }
    return false;
  }

  @Override
  public List<String> preferredLocations() {
    return this.preferredLocations;
  }

  /**
   * Data Lake formats require some additional processing to be done on the FileBuilder, such as
   * inserting delete files information. Different lake formats should override this method to
   * implement their corresponding logic.
   */
  protected void processFileBuilder(
      ReadRel.LocalFiles.FileOrFiles.Builder fileBuilder, int index) {}

  @Override
  public ReadRel.LocalFiles toProtobuf() {
    ReadRel.LocalFiles.Builder localFilesBuilder = ReadRel.LocalFiles.newBuilder();
    // The input is iterator, and the path is in the format of: Iterator:index.
    if (iterAsInput && !paths.isEmpty()) {
      ReadRel.LocalFiles.FileOrFiles.Builder fileBuilder =
          ReadRel.LocalFiles.FileOrFiles.newBuilder();
      fileBuilder.setUriFile(paths.get(0));
      localFilesBuilder.addItems(fileBuilder.build());
      return localFilesBuilder.build();
    }
    if (paths.size() != starts.size() || paths.size() != lengths.size()) {
      throw new RuntimeException("Invalid parameters.");
    }
    for (int i = 0; i < paths.size(); i++) {
      ReadRel.LocalFiles.FileOrFiles.Builder fileBuilder =
          ReadRel.LocalFiles.FileOrFiles.newBuilder();
      fileBuilder.setUriFile(paths.get(i));
      if (index != null) {
        fileBuilder.setPartitionIndex(index);
      }
      Map<String, String> partitionColumn = partitionColumns.get(i);
      if (!partitionColumn.isEmpty()) {
        partitionColumn.forEach(
            (key, value) -> {
              ReadRel.LocalFiles.FileOrFiles.partitionColumn.Builder pcBuilder =
                  ReadRel.LocalFiles.FileOrFiles.partitionColumn.newBuilder();
              pcBuilder.setKey(key).setValue(value);
              fileBuilder.addPartitionColumns(pcBuilder.build());
            });
      }
      fileBuilder.setLength(lengths.get(i));
      fileBuilder.setStart(starts.get(i));

      if (!fileSizes.isEmpty()
          && !modificationTimes.isEmpty()
          && fileSizes.size() == modificationTimes.size()
          && fileSizes.size() == paths.size()) {
        ReadRel.LocalFiles.FileOrFiles.fileProperties.Builder filePropsBuilder =
            ReadRel.LocalFiles.FileOrFiles.fileProperties.newBuilder();
        filePropsBuilder.setFileSize(fileSizes.get(i));
        filePropsBuilder.setModificationTime(modificationTimes.get(i));
        fileBuilder.setProperties(filePropsBuilder.build());
      }

      if (!metadataColumns.isEmpty()) {
        Map<String, String> metadataColumn = metadataColumns.get(i);
        if (!metadataColumn.isEmpty()) {
          metadataColumn.forEach(
              (key, value) -> {
                ReadRel.LocalFiles.FileOrFiles.metadataColumn.Builder mcBuilder =
                    ReadRel.LocalFiles.FileOrFiles.metadataColumn.newBuilder();
                mcBuilder.setKey(key).setValue(value);
                fileBuilder.addMetadataColumns(mcBuilder.build());
              });
        }
      } else {
        ReadRel.LocalFiles.FileOrFiles.metadataColumn.Builder mcBuilder =
            ReadRel.LocalFiles.FileOrFiles.metadataColumn.newBuilder();
        fileBuilder.addMetadataColumns(mcBuilder.build());
      }

      if (fileSchema != null) {
        NamedStruct namedStruct = buildNamedStruct();
        fileBuilder.setSchema(namedStruct);
      }

      if (!otherMetadataColumns.isEmpty()) {
        Map<String, Object> otherMetadatas = otherMetadataColumns.get(i);
        if (!otherMetadatas.isEmpty()) {
          otherMetadatas.forEach(
              (key, value) -> {
                ReadRel.LocalFiles.FileOrFiles.otherConstantMetadataColumnValues.Builder builder =
                    ReadRel.LocalFiles.FileOrFiles.otherConstantMetadataColumnValues.newBuilder();
                builder.setKey(key).setValue(SubstraitUtil.convertJavaObjectToAny(value));
                fileBuilder.addOtherConstMetadataColumns(builder.build());
              });
        }
      }

      switch (fileFormat) {
        case ParquetReadFormat:
          ReadRel.LocalFiles.FileOrFiles.ParquetReadOptions parquetReadOptions =
              ReadRel.LocalFiles.FileOrFiles.ParquetReadOptions.newBuilder().build();
          fileBuilder.setParquet(parquetReadOptions);
          break;
        case OrcReadFormat:
          ReadRel.LocalFiles.FileOrFiles.OrcReadOptions orcReadOptions =
              ReadRel.LocalFiles.FileOrFiles.OrcReadOptions.newBuilder().build();
          fileBuilder.setOrc(orcReadOptions);
          break;
        case DwrfReadFormat:
          ReadRel.LocalFiles.FileOrFiles.DwrfReadOptions dwrfReadOptions =
              ReadRel.LocalFiles.FileOrFiles.DwrfReadOptions.newBuilder().build();
          fileBuilder.setDwrf(dwrfReadOptions);
          break;
        case TextReadFormat:
          String field_delimiter = fileReadProperties.getOrDefault("field_delimiter", ",");
          String quote = fileReadProperties.getOrDefault("quote", "");
          String header = fileReadProperties.getOrDefault("header", "0");
          String escape = fileReadProperties.getOrDefault("escape", "");
          String nullValue = fileReadProperties.getOrDefault("nullValue", "");
          ReadRel.LocalFiles.FileOrFiles.TextReadOptions textReadOptions =
              ReadRel.LocalFiles.FileOrFiles.TextReadOptions.newBuilder()
                  .setFieldDelimiter(field_delimiter)
                  .setQuote(quote)
                  .setHeader(Long.parseLong(header))
                  .setEscape(escape)
                  .setNullValue(nullValue)
                  .setMaxBlockSize(GlutenConfig.get().textInputMaxBlockSize())
                  .setEmptyAsDefault(GlutenConfig.get().textIputEmptyAsDefault())
                  .build();
          fileBuilder.setText(textReadOptions);
          break;
        case JsonReadFormat:
          ReadRel.LocalFiles.FileOrFiles.JsonReadOptions jsonReadOptions =
              ReadRel.LocalFiles.FileOrFiles.JsonReadOptions.newBuilder()
                  .setMaxBlockSize(GlutenConfig.get().textInputMaxBlockSize())
                  .build();
          fileBuilder.setJson(jsonReadOptions);
          break;
        default:
          break;
      }
      processFileBuilder(fileBuilder, i);
      localFilesBuilder.addItems(fileBuilder.build());
    }
    return localFilesBuilder.build();
  }
}

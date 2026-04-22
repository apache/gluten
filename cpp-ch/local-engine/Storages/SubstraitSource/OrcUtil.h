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
#pragma once
#include <Processors/Formats/Impl/ArrowColumnToCHColumn.h>
#include <Processors/Formats/Impl/ORCBlockInputFormat.h>

#include <arrow/io/interfaces.h>
#include <atomic>

/// there are destructor not be overrided warnings in orc lib, ignore them
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsuggest-destructor-override"
#include <orc/OrcFile.hh>
#include <orc/Reader.hh>
#pragma GCC diagnostic pop
#include <memory>
#include <orc/Exceptions.hh>

namespace local_engine
{
class ArrowInputFile : public orc::InputStream
{
public:
    explicit ArrowInputFile(const std::shared_ptr<arrow::io::RandomAccessFile> & file_) : file(file_) { }

    uint64_t getLength() const override;

    uint64_t getNaturalReadSize() const override;

    void read(void * buf, uint64_t length, uint64_t offset) override;

    const std::string & getName() const override;

private:
    std::shared_ptr<arrow::io::RandomAccessFile> file;
};

class OrcUtil
{
public:
    /// Same as ParquetMetaBuilder::openInputParquetFile: on HDFS EC, Arrow's default ReadAt is Seek()+Read();
    /// ORC postscript/footer reads need readBigAt (pread) for correct tail bytes.
    static std::shared_ptr<arrow::io::RandomAccessFile> openArrowRandomAccessFileForOrc(
        DB::ReadBuffer & read_buffer,
        const DB::FormatSettings & format_settings,
        std::atomic<int> & is_stopped);

    static std::unique_ptr<orc::Reader> createOrcReader(std::shared_ptr<arrow::io::RandomAccessFile> file_);

    static size_t countIndicesForType(std::shared_ptr<arrow::DataType> type);
    static void getFileReaderAndSchema(
        DB::ReadBuffer & in,
        std::unique_ptr<arrow::adapters::orc::ORCFileReader> & file_reader,
        std::shared_ptr<arrow::Schema> & schema,
        const DB::FormatSettings & format_settings,
        std::atomic<int> & is_stopped);
};

}

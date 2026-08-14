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

#include <arrow/adapters/orc/adapter.h>
#include "operators/reader/ParquetReaderIterator.h"

namespace gluten {

class OrcConverter final {
 public:
  explicit OrcConverter(const std::vector<std::string>& inputFiles) : inputFiles_(inputFiles) {
    orcFiles_.resize(inputFiles.size());
  }

  const std::vector<std::string>& getOrcFiles() {
    for (auto i = 0; i != inputFiles_.size(); ++i) {
      GLUTEN_ASSIGN_OR_THROW(orcFiles_[i], createOrcFile(inputFiles_[i]));
    }
    return orcFiles_;
  }

 private:
  arrow::Result<std::string> createOrcFile(const std::string& inputFile) {
    ParquetStreamReaderIterator parquetIterator(inputFile);

    std::string outputFile = inputFile;
    // Get the filename.
    auto pos = inputFile.find_last_of("/");
    if (pos != std::string::npos) {
      outputFile = inputFile.substr(pos + 1);
    }
    // If any suffix is found, replace it with ".orc"
    pos = outputFile.find_first_of(".");
    if (pos != std::string::npos) {
      outputFile = outputFile.substr(0, pos) + kOrcSuffix;
    } else {
      return arrow::Status::Invalid("Invalid input file: " + inputFile);
    }
    outputFile = std::filesystem::current_path().string() + "/" + outputFile;

    std::shared_ptr<arrow::io::FileOutputStream> outputStream;
    ARROW_ASSIGN_OR_RAISE(outputStream, arrow::io::FileOutputStream::Open(outputFile));

    auto writerOptions = arrow::adapters::orc::WriteOptions();
    auto maybeWriter = arrow::adapters::orc::ORCFileWriter::Open(outputStream.get(), writerOptions);
    GLUTEN_THROW_NOT_OK(maybeWriter);
    auto& writer = *maybeWriter;

    // Read from parquet and write to ORC.
    while (auto cb = parquetIterator.next()) {
      GLUTEN_ASSIGN_OR_THROW(
          auto recordBatch, arrow::ImportRecordBatch(cb->exportArrowArray().get(), parquetIterator.getSchema()));
      if (!(writer->Write(*recordBatch)).ok()) {
        return arrow::Status::IOError("Write failed");
      }
    }

    if (!(writer->Close()).ok()) {
      return arrow::Status::IOError("Close failed");
    }
    return outputFile;
  }

  std::vector<std::string> inputFiles_;
  std::vector<std::string> orcFiles_;
};

} // namespace gluten

int main(int argc, char** argv) {
  if (argc == 1) {
    std::cout << "Please specify parquet files as input arguments." << std::endl;
    exit(0);
  }

  std::vector<std::string> inputFiles;
  for (auto i = 1; i < argc; ++i) {
    const auto& file = argv[i];
    if (!std::filesystem::exists(file)) {
      std::cout << file << " doesn't exist!" << std::endl;
      exit(1);
    }
    inputFiles.emplace_back(argv[i]);
  }

  auto orcConverter = std::make_shared<gluten::OrcConverter>(inputFiles);
  auto orcFiles = orcConverter->getOrcFiles();
  std::cout << "Generated output files: " << std::endl;
  for (const auto& file : orcFiles) {
    std::cout << file << std::endl;
  }
  return 0;
}

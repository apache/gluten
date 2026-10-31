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

#include "PaimonFormatFile.h"
#include "PaimonReader.h"

#include <Common/Exception.h>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace local_engine
{

PaimonFormatFile::PaimonFormatFile(
    DB::ContextPtr context, const substrait::ReadRel::LocalFiles::FileOrFiles & file, const ReadBufferBuilderPtr & read_buffer_builder)
    : FormatFile(context, file, read_buffer_builder)
{
    if (!file.has_paimon() || !file.paimon().has_cpp_split() || file.paimon().cpp_split().table_path().empty()
        || file.paimon().cpp_split().serialized_data_split().empty())
        throw DB::Exception(
            DB::ErrorCodes::BAD_ARGUMENTS,
            "Paimon native read requires table_path and serialized_data_split in substrait LocalFiles");
}

FormatFile::InputFormatPtr PaimonFormatFile::createInputFormat(
    const DB::Block & header, const std::shared_ptr<const DB::ActionsDAG> & /*filter_actions_dag*/)
{
    return paimon::createPaimonInputFormat(*this, header);
}

}

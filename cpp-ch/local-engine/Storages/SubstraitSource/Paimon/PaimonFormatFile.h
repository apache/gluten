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

#include <Storages/SubstraitSource/FormatFile.h>

namespace local_engine
{
/// Carries Substrait Paimon read options and creates the native input format.
class PaimonFormatFile final : public FormatFile
{
public:
    PaimonFormatFile(
        DB::ContextPtr context,
        const substrait::ReadRel::LocalFiles::FileOrFiles & file,
        const ReadBufferBuilderPtr & read_buffer_builder);

    InputFormatPtr
    createInputFormat(const DB::Block & header, const std::shared_ptr<const DB::ActionsDAG> & filter_actions_dag = nullptr) override;

    String getFileFormat() const override { return "Paimon"; }

    bool supportSplit() const override { return false; }
};

}

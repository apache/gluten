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
#include "ORCFormatFile.h"

#if USE_ORC
#include <memory>
#include <numeric>
#include <Formats/FormatFactory.h>
#include <IO/SeekableReadBuffer.h>
#include <Processors/Formats/Impl/ArrowBufferedStreams.h>
#include <Processors/Formats/Impl/NativeORCBlockInputFormat.h>
#include <Storages/SubstraitSource/OrcUtil.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/BlockTypeUtils.h>
#include <Common/CHUtil.h>

namespace local_engine
{

ORCFormatFile::ORCFormatFile(
    DB::ContextPtr context_, const substrait::ReadRel::LocalFiles::FileOrFiles & file_info_, ReadBufferBuilderPtr read_buffer_builder_)
    : FormatFile(context_, file_info_, read_buffer_builder_)
{
}

FormatFile::InputFormatPtr
ORCFormatFile::createInputFormat(const DB::Block & header, const std::shared_ptr<const DB::ActionsDAG> & filter_actions_dag)
{
    auto read_buffer = read_buffer_builder->build(file_info);

    std::vector<StripeInformation> stripes;
    [[maybe_unused]] UInt64 total_stripes = 0;
    if (auto * seekable_in = dynamic_cast<DB::SeekableReadBuffer *>(read_buffer.get()))
    {
        stripes = collectRequiredStripes(seekable_in, total_stripes);
        seekable_in->seek(0, SEEK_SET);
    }
    else
        stripes = collectRequiredStripes(total_stripes);

    auto format_settings = DB::getFormatSettings(context);

    std::vector<int> total_stripe_indices(total_stripes);
    std::iota(total_stripe_indices.begin(), total_stripe_indices.end(), 0);

    std::vector<UInt64> required_stripe_indices(stripes.size());
    for (size_t i = 0; i < stripes.size(); ++i)
        required_stripe_indices[i] = stripes[i].index;

    std::vector<int> skip_stripe_indices;
    std::ranges::set_difference(total_stripe_indices, required_stripe_indices, std::back_inserter(skip_stripe_indices));

    format_settings.orc.skip_stripes = std::unordered_set<int>(skip_stripe_indices.begin(), skip_stripe_indices.end());
    if (context->getConfigRef().has("timezone"))
    {
        const String config_timezone = context->getConfigRef().getString("timezone");
        const String mapped_timezone = DateTimeUtil::convertTimeZone(config_timezone);
        format_settings.orc.reader_time_zone_name = mapped_timezone;
    }
    //TODO: support prefetch
    auto parser_group = std::make_shared<DB::FormatFilterInfo>(filter_actions_dag, context, nullptr);
    auto input_format
        = std::make_shared<DB::NativeORCBlockInputFormat>(*read_buffer, toShared(header), format_settings, false, 0, parser_group);
    return std::make_shared<InputFormat>(std::move(read_buffer), input_format);
}

std::optional<size_t> ORCFormatFile::getTotalRows()
{
    {
        std::lock_guard lock(mutex);
        if (total_rows)
            return total_rows;
    }

    UInt64 _;
    auto required_stripes = collectRequiredStripes(_);

    {
        std::lock_guard lock(mutex);
        if (total_rows)
            return total_rows;

        size_t num_rows = 0;
        for (const auto stipe_info : required_stripes)
            num_rows += stipe_info.num_rows;

        total_rows = num_rows;
        return total_rows;
    }
}

std::vector<StripeInformation> ORCFormatFile::collectRequiredStripes(UInt64 & total_stripes)
{
    auto in = read_buffer_builder->build(file_info);
    return collectRequiredStripes(in.get(), total_stripes);
}

std::vector<StripeInformation> ORCFormatFile::collectRequiredStripes(DB::ReadBuffer * read_buffer, UInt64 & total_stripes)
{
    DB::FormatSettings format_settings{
        .seekable_read = true,
    };
    std::atomic<int> is_stopped{0};
    auto arrow_file = DB::asArrowFile(*read_buffer, format_settings, is_stopped, "ORC", ORC_MAGIC_BYTES);
    auto orc_reader = OrcUtil::createOrcReader(arrow_file);
    total_stripes = orc_reader->getNumberOfStripes();

    size_t total_num_rows = 0;
    std::vector<StripeInformation> stripes;
    stripes.reserve(total_stripes);
    for (size_t i = 0; i < total_stripes; ++i)
    {
        auto stripe_metadata = orc_reader->getStripe(i);

        auto offset = stripe_metadata->getOffset() + stripe_metadata->getLength() / 2;
        if (file_info.start() <= offset && offset < file_info.start() + file_info.length())
        {
            StripeInformation stripe_info;
            stripe_info.index = i;
            stripe_info.offset = stripe_metadata->getLength();
            stripe_info.length = stripe_metadata->getLength();
            stripe_info.num_rows = stripe_metadata->getNumberOfRows();
            stripe_info.start_row = total_num_rows;
            stripes.emplace_back(stripe_info);
        }

        total_num_rows += stripe_metadata->getNumberOfRows();
    }
    return stripes;
}
}
#endif

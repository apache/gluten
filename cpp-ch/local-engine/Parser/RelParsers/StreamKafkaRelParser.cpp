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

#include "StreamKafkaRelParser.h"

#include <Parser/SubstraitParserUtils.h>
#include <Parser/TypeParser.h>
#include <Storages/Kafka/ReadFromGlutenStorageKafka.h>
#include <kafka.pb.h>
#include <Common/BlockTypeUtils.h>
#include <Common/DebugUtils.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
extern const int NO_SUCH_DATA_PART;
extern const int LOGICAL_ERROR;
extern const int CANNOT_PARSE_PROTOBUF_SCHEMA;
extern const int UNKNOWN_FUNCTION;
extern const int UNKNOWN_TYPE;
}
}


namespace local_engine
{

DB::QueryPlanPtr
StreamKafkaRelParser::parse(DB::QueryPlanPtr query_plan, const substrait::Rel & rel, std::list<const substrait::Rel *> & rel_stack_)
{
    if (rel.has_read())
        return parseRelImpl(std::move(query_plan), rel.read());

    throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "StreamKafkaRelParser can't parse rel:{}", rel.ShortDebugString());
}

DB::QueryPlanPtr StreamKafkaRelParser::parseRelImpl(DB::QueryPlanPtr query_plan, const substrait::ReadRel & read_rel)
{
    if (split_info.empty())
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Can't parse kafka rel, because no split info was supplied for it");

    auto extension_table = BinaryToMessage<substrait::ReadRel::ExtensionTable>(split_info);
    debug::dumpMessage(extension_table, "extension_table");

    gluten::StreamKafka kafka_task;
    if (!extension_table.detail().UnpackTo(&kafka_task))
        throw DB::Exception(
            DB::ErrorCodes::CANNOT_PARSE_PROTOBUF_SCHEMA,
            "Can't parse kafka rel, expected an extension table detail of type gluten.StreamKafka but got '{}'",
            extension_table.detail().type_url());

    auto topic = kafka_task.topic_partition().topic();
    auto partition = kafka_task.topic_partition().partition();
    auto start_offset = kafka_task.start_offset();
    auto end_offset = kafka_task.end_offset();
    auto poll_timeout_ms = kafka_task.poll_timeout_ms();
    String group_id;
    String brokers;

    for (auto param : kafka_task.params())
        if (param.first == "poll_timeout_ms")
            poll_timeout_ms = std::stoi(param.second);
        else if (param.first == "group.id")
            group_id = param.second;
        else if (param.first == "bootstrap.servers")
            brokers = param.second;
        else
            LOG_DEBUG(getLogger("StreamKafkaRelParser"), "Unused kafka parameter: {}: {}", param.first, param.second);

    LOG_INFO(
        getLogger("StreamKafkaRelParser"),
        "Kafka source: topic: {}, partition: {}, start_offset: {}, end_offset: {}",
        topic,
        partition,
        start_offset,
        end_offset);

    if (group_id.empty())
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Kafka group.id is not set");

    Names topics;
    topics.emplace_back(topic);

    auto header = toShared(TypeParser::buildBlockFromNamedStruct(read_rel.base_schema()));
    Names names = header->getNames();
    auto source = std::make_unique<ReadFromGlutenStorageKafka>(
        names, header, getContext(), topics, partition, start_offset, end_offset, poll_timeout_ms, group_id, brokers);

    steps.emplace_back(source.get());
    query_plan->addStep(std::move(source));

    return query_plan;
}


}
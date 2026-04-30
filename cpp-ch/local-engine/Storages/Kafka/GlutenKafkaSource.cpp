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

#include "GlutenKafkaSource.h"

#include <Columns/IColumn.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeNullable.h>
#include <Formats/FormatFactory.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Processors/Executors/StreamingFormatExecutor.h>
#include <Storages/Kafka/GlutenKafkaUtils.h>
#include <Storages/Kafka/KafkaConfigLoader.h>
#include <Storages/Kafka/KafkaSettings.h>
#include <Storages/Kafka/parseSyslogLevel.h>
#include <boost/algorithm/string/replace.hpp>
#include <Common/NamedCollections/NamedCollectionsFactory.h>

namespace ProfileEvents
{
extern const Event KafkaMessagesRead;
extern const Event KafkaMessagesFailed;
extern const Event KafkaRowsRead;
extern const Event KafkaRowsRejected;
}

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace Setting
{
extern const SettingsUInt64 max_block_size;
extern const SettingsUInt64 max_insert_block_size;
extern const SettingsUInt64 output_format_avro_rows_in_file;
extern const SettingsMilliseconds stream_flush_interval_ms;
extern const SettingsMilliseconds stream_poll_timeout_ms;
extern const SettingsBool use_concurrency_control;
}

namespace KafkaSetting
{
extern const KafkaSettingsUInt64 input_format_allow_errors_num;
extern const KafkaSettingsFloat input_format_allow_errors_ratio;
extern const KafkaSettingsString kafka_broker_list;
extern const KafkaSettingsString kafka_client_id;
extern const KafkaSettingsBool kafka_commit_every_batch;
extern const KafkaSettingsBool kafka_commit_on_select;
extern const KafkaSettingsUInt64 kafka_consumers_pool_ttl_ms;
extern const KafkaSettingsMilliseconds kafka_flush_interval_ms;
extern const KafkaSettingsString kafka_format;
extern const KafkaSettingsString kafka_group_name;
extern const KafkaSettingsStreamingHandleErrorMode kafka_handle_error_mode;
extern const KafkaSettingsUInt64 kafka_max_block_size;
extern const KafkaSettingsUInt64 kafka_max_rows_per_message;
extern const KafkaSettingsUInt64 kafka_num_consumers;
extern const KafkaSettingsUInt64 kafka_poll_max_batch_size;
extern const KafkaSettingsMilliseconds kafka_poll_timeout_ms;
extern const KafkaSettingsString kafka_schema;
extern const KafkaSettingsBool kafka_thread_per_consumer;
extern const KafkaSettingsString kafka_topic_list;
}
}


namespace local_engine
{

size_t GlutenKafkaSource::getPollMaxBatchSize() const
{
    size_t batch_size = (*kafka_settings)[KafkaSetting::kafka_poll_max_batch_size].changed
        ? (*kafka_settings)[KafkaSetting::kafka_poll_max_batch_size].value
        : context->getSettingsRef()[Setting::max_block_size].value;

    return std::min(batch_size, getMaxBlockSize());
}

size_t GlutenKafkaSource::getMaxBlockSize() const
{
    return (*kafka_settings)[KafkaSetting::kafka_max_block_size].changed
        ? (*kafka_settings)[KafkaSetting::kafka_max_block_size].value
        : (context->getSettingsRef()[Setting::max_insert_block_size].value / /*num_consumers*/ 1);
}

size_t GlutenKafkaSource::getPollTimeoutMillisecond() const
{
    return (*kafka_settings)[KafkaSetting::kafka_poll_timeout_ms].changed
        ? (*kafka_settings)[KafkaSetting::kafka_poll_timeout_ms].totalMilliseconds()
        : context->getSettingsRef()[Setting::stream_poll_timeout_ms].totalMilliseconds();
}

GlutenKafkaSource::GlutenKafkaSource(
    const DB::SharedHeader & result_header_,
    const ContextPtr & context_,
    const Names & topics_,
    const size_t & partition_,
    const String & brokers_,
    const String & group_,
    const size_t & poll_timeout_ms_,
    const size_t & start_offset_,
    const size_t & end_offset_,
    const std::shared_ptr<KafkaSettings> & kafka_settings_)
    : ISource(result_header_)
    , context(context_)
    , log(getLogger("GlutenKafkaSource"))
    , kafka_settings(kafka_settings_)
    , result_header(result_header_)
    , topics(topics_)
    , brokers(brokers_)
    , group(group_)
    , poll_timeout_ms(poll_timeout_ms_)
    , start_offset(start_offset_)
    , end_offset(end_offset_)
    , partition(partition_)
{
    max_block_size = end_offset - start_offset;
    client_id = topics[0] + "_" + std::to_string(partition);

    for (const auto & columns_with_type_and_name : result_header->getColumnsWithTypeAndName())
    {
        if (columns_with_type_and_name.name == "value")
        {
            const auto no_null_datatype = removeNullable(columns_with_type_and_name.type);
            non_virtual_header.insert(ColumnWithTypeAndName(no_null_datatype->createColumn(), no_null_datatype, "value"));
            continue;
        }

        virtual_header.insert(columns_with_type_and_name);
    }
}

GlutenKafkaSource::~GlutenKafkaSource()
{
    std::lock_guard lock(consumer_mutex);
    auto topic_partition = TopicPartition{topics[0], partition};
    consumers_in_memory[topic_partition].emplace_back(consumer);
    LOG_DEBUG(
        log,
        "Kafka consumer for topic: {}, partition: {} is returned to pool, current pool size: {}",
        topics[0],
        partition,
        consumers_in_memory[topic_partition].size());
}

void GlutenKafkaSource::initConsumer()
{
    std::lock_guard lock(consumer_mutex);
    auto topic_partition = TopicPartition{topics[0], partition};
    consumers_in_memory.try_emplace(topic_partition, std::vector<std::shared_ptr<DB::KafkaConsumer>>());

    auto & consumers = consumers_in_memory[topic_partition];

    if (!consumers.empty())
    {
        LOG_DEBUG(log, "Reuse Kafka consumer for topic: {}, partition: {}", topics[0], partition);
        consumer = consumers.back();
        consumers.pop_back();
    }

    if (!consumer)
    {
        LOG_DEBUG(log, "Creating new Kafka consumer for topic: {}, partition: {}", topics[0], partition);
        String collection_name = "";
        std::shared_ptr<DB::KafkaConsumer> kafka_consumer_ptr = std::make_shared<KafkaConsumer>(
            log,
            getPollMaxBatchSize(),
            getPollTimeoutMillisecond(),
            /*intermediate_commit*/ false,
            /*stream_cancelled*/ is_stopped,
            topics);

        KafkaConfigLoader::ConsumerConfigParams params{
            {context->getConfigRef(), /*collection_name*/ collection_name, topics, log},
            brokers,
            group,
            false,
            1,
            client_id,
            getMaxBlockSize()};

        kafka_consumer_ptr->createConsumer(GlutenKafkaUtils::getConsumerConfiguration(params));
        consumer = kafka_consumer_ptr;
        LOG_DEBUG(log, "Created new Kafka consumer for topic: {}, partition: {}", topics[0], partition);
    }

    consumer->subscribe(topics[0], partition, start_offset);
}


Chunk GlutenKafkaSource::generateImpl()
{
    if (!consumer)
        initConsumer();

    size_t batch_rows = 0;
    MutableColumns virtual_columns = virtual_header.cloneEmptyColumns();
    MutableColumns no_virtual_columns = non_virtual_header.cloneEmptyColumns();

    while (true)
    {
        if (auto buf = consumer->consume())
        {
            String message;
            readStringUntilEOF(message, *buf);
            no_virtual_columns[0]->insert(message);

            // In read_kafka_message(), KafkaConsumer::nextImpl()
            // will be called, that may make something unusable, i.e. clean
            // KafkaConsumer::messages, which is accessed from
            // KafkaConsumer::currentTopic() (and other helpers).
            if (consumer->isStalled())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Polled messages became unusable");

            ProfileEvents::increment(ProfileEvents::KafkaRowsRead, 1);
            consumer->storeLastReadMessageOffset();

            auto topic = consumer->currentTopic();
            auto key = consumer->currentKey();
            auto offset = consumer->currentOffset();
            auto partition = consumer->currentPartition();
            auto timestamp_raw = consumer->currentTimestamp();
            auto header_list = consumer->currentHeaderList();

            virtual_columns[0]->insert(key);
            virtual_columns[1]->insert(topic);
            virtual_columns[2]->insert(partition);
            virtual_columns[3]->insert(offset);

            if (timestamp_raw)
            {
                auto ts = timestamp_raw->get_timestamp();
                virtual_columns[4]->insert(
                    DecimalField<Decimal64>(std::chrono::duration_cast<std::chrono::milliseconds>(ts).count(), 3));
            }
            else
            {
                virtual_columns[4]->insertDefault();
            }

            virtual_columns[5]->insertDefault();

            batch_rows = batch_rows + 1;
        }
        else if (consumer->polledDataUnusable())
        {
            break;
        }
        else
        {
            // We came here in case of tombstone (or sometimes zero-length) messages, and it is not something abnormal
            // TODO: it seems like in case of put_error_to_stream=true we may need to process those differently
            // currently we just skip them with note in logs.
            consumer->storeLastReadMessageOffset();
            LOG_DEBUG(
                log,
                "Parsing of message (topic: {}, partition: {}, offset: {}) return no rows.",
                consumer->currentTopic(),
                consumer->currentPartition(),
                consumer->currentOffset());
        }

        if (!consumer->hasMorePolledMessages() || total_rows + batch_rows >= max_block_size)
            break;
    }

    LOG_DEBUG(log, "Read {} rows from Kafka topic: {}, partition: {}", batch_rows, topics[0], partition);

    if (batch_rows == 0)
    {
        finished = true;
        return {};
    }

    if (consumer->polledDataUnusable())
    {
        // the rows were counted already before by KafkaRowsRead,
        // so let's count the rows we ignore separately
        // (they will be retried after the rebalance)
        ProfileEvents::increment(ProfileEvents::KafkaRowsRejected, batch_rows);
        return {};
    }

    auto result_block = non_virtual_header.cloneWithColumns(std::move(no_virtual_columns)); //.cloneWithCutColumns(0, max_block_size);

    auto virtual_block = virtual_header.cloneWithColumns(std::move(virtual_columns)); //.cloneWithCutColumns(0, max_block_size);
    for (const auto & column : virtual_block.getColumnsWithTypeAndName())
        result_block.insert(column);

    progress(batch_rows, result_block.bytes());

    auto converting_dag = ActionsDAG::makeConvertingActions(
        result_block.cloneEmpty().getColumnsWithTypeAndName(),
        getPort().getHeader().getColumnsWithTypeAndName(),
        ActionsDAG::MatchColumnsMode::Name,
        context);

    auto converting_actions = std::make_shared<ExpressionActions>(std::move(converting_dag));
    converting_actions->execute(result_block);

    total_rows += batch_rows;
    return Chunk(result_block.getColumns(), result_block.rows());
}

Chunk GlutenKafkaSource::generate()
{
    if (isCancelled() || finished)
        return {};

    auto chunk = generateImpl();
    if (total_rows >= max_block_size)
        finished = true;

    return chunk;
}
}

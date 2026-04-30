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
#include "RelMetric.h"

#include <Processors/IProcessor.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Storages/SubstraitSource/SubstraitFileSourceStep.h>
#include <Common/QueryContext.h>

using namespace rapidjson;

namespace ProfileEvents
{
extern const Event FileSegmentWaitReadBufferMicroseconds;
extern const Event FileSegmentReadMicroseconds;
extern const Event FileSegmentCacheWriteMicroseconds;
extern const Event FileSegmentPredownloadMicroseconds;
extern const Event FileSegmentUsedBytes;

extern const Event CachedReadBufferReadFromSourceMicroseconds;
extern const Event CachedReadBufferReadFromCacheMicroseconds;
extern const Event CachedReadBufferCacheWriteMicroseconds;
extern const Event CachedReadBufferReadFromSourceBytes;
extern const Event CachedReadBufferReadFromCacheBytes;
extern const Event CachedReadBufferCacheWriteBytes;
extern const Event CachedReadBufferCreateBufferMicroseconds;

extern const Event CachedReadBufferReadFromCacheHits;
extern const Event CachedReadBufferReadFromCacheMisses;
}

namespace local_engine
{

static void writeCacheHits(Writer<StringBuffer> & writer)
{
    const auto thread_group = QueryContext::currentThreadGroup();
    auto & counters = thread_group->performance_counters;
    auto read_cache_hits = counters[ProfileEvents::CachedReadBufferReadFromCacheHits].load();
    auto miss_cache_hits = counters[ProfileEvents::CachedReadBufferReadFromCacheMisses].load();
    auto read_cache_bytes = counters[ProfileEvents::CachedReadBufferReadFromCacheBytes].load();
    auto read_miss_bytes = counters[ProfileEvents::CachedReadBufferReadFromSourceBytes].load();
    auto read_cache_millisecond = counters[ProfileEvents::CachedReadBufferReadFromCacheMicroseconds].load() / 1000;
    auto miss_cache_millisecond = counters[ProfileEvents::CachedReadBufferReadFromSourceMicroseconds].load() / 1000;

    writer.Key("read_cache_hits");
    writer.Uint64(read_cache_hits);
    writer.Key("miss_cache_hits");
    writer.Uint64(miss_cache_hits);
    writer.Key("read_cache_bytes");
    writer.Uint64(read_cache_bytes);
    writer.Key("read_miss_bytes");
    writer.Uint64(read_miss_bytes);
    writer.Key("read_cache_millisecond");
    writer.Uint64(read_cache_millisecond);
    writer.Key("miss_cache_millisecond");
    writer.Uint64(miss_cache_millisecond);
}

RelMetric::RelMetric(size_t id_, const String & name_, std::vector<DB::IQueryPlanStep *> & steps_) : id(id_), name(name_), steps(steps_)
{
}

RelMetric::RelMetric(const String & name_, const std::vector<RelMetricPtr> & inputs_, std::vector<DB::IQueryPlanStep *> & steps_)
    : name(name_), steps(steps_), inputs(inputs_)
{
    auto rel = std::max_element(inputs.begin(), inputs.end(), [](RelMetricPtr a, RelMetricPtr b) { return a->id < b->id; });
    id = rel->get()->id + 1;
}

size_t RelMetric::getId() const
{
    return id;
}

const std::vector<DB::IQueryPlanStep *> & RelMetric::getSteps() const
{
    return steps;
}

const std::vector<RelMetricPtr> & RelMetric::getInputs() const
{
    return inputs;
}

RelMetricTimes RelMetric::getTotalTime() const
{
    RelMetricTimes timeMetrics{0, 0, 0};
    if (!steps.empty())
    {
        for (const auto * step : steps)
        {
            if (!step->getProcessors().empty())
            {
                for (const auto & processor : step->getProcessors())
                {
                    timeMetrics.time += processor->getElapsedNs() / 1000U;
                    timeMetrics.input_wait_elapsed_us += processor->getInputWaitElapsedNs() / 1000U;
                    timeMetrics.output_wait_elapsed_us += processor->getInputWaitElapsedNs() / 1000U;
                }
            }
        }
    }
    return timeMetrics;
}

void RelMetric::serialize(Writer<StringBuffer> & writer, bool) const
{
    writer.StartObject();
    writer.Key("id");
    writer.Uint64(id);
    writer.Key("name");
    writer.String(name.c_str());
    RelMetricTimes timeMetrics = getTotalTime();
    writer.Key("time");
    writer.Uint64(timeMetrics.time);
    writer.Key("input_wait_time");
    writer.Uint64(timeMetrics.input_wait_elapsed_us);
    writer.Key("output_wait_time");
    writer.Uint64(timeMetrics.output_wait_elapsed_us);
    if (!steps.empty())
    {
        writer.Key("steps");
        writer.StartArray();
        for (const auto & step : steps)
        {
            writer.StartObject();
            writer.Key("name");
            writer.String(step->getName().c_str());
            writer.Key("description");
            writer.String(step->getStepDescription().data());
            writer.Key("processors");
            writer.StartArray();
            for (const auto & processor : step->getProcessors())
            {
                writer.StartObject();
                writer.Key("name");
                writer.String(processor->getName().c_str());
                writer.Key("time");
                writer.Uint64(processor->getElapsedNs() / 1000U);
                writer.Key("output_rows");
                writer.Uint64(processor->getProcessorDataStats().output_rows);
                writer.Key("output_bytes");
                writer.Uint64(processor->getProcessorDataStats().output_bytes);
                writer.Key("input_rows");
                writer.Uint64(processor->getProcessorDataStats().input_rows);
                writer.Key("input_bytes");
                writer.Uint64(processor->getProcessorDataStats().input_bytes);
                writer.EndObject();
            }
            writer.EndArray();

            if (auto read_mergetree = dynamic_cast<DB::ReadFromMergeTree *>(step))
            {
                auto selected_marks_pk = read_mergetree->getAnalysisResult().selected_marks_pk;
                auto selected_marks = read_mergetree->getAnalysisResult().selected_marks;
                auto total_marks_pk = read_mergetree->getAnalysisResult().total_marks_pk;
                writer.Key("selected_marks_pk");
                writer.Uint64(selected_marks_pk);
                writer.Key("selected_marks");
                writer.Uint64(selected_marks);
                writer.Key("total_marks_pk");
                writer.Uint64(total_marks_pk);
                writeCacheHits(writer);
            }
            else if (dynamic_cast<SubstraitFileSourceStep *>(step))
            {
                writeCacheHits(writer);
            }

            writer.EndObject();
        }
        writer.EndArray();
    }
    writer.EndObject();
}

const String & RelMetric::getName() const
{
    return name;
}

std::string RelMetricSerializer::serializeRelMetric(const RelMetricPtr & rel_metric, bool flatten)
{
    StringBuffer result;
    Writer<StringBuffer> writer(result);
    if (flatten)
    {
        writer.StartArray();
        std::stack<RelMetricPtr> metrics;
        metrics.push(rel_metric);
        while (!metrics.empty())
        {
            auto metric = metrics.top();
            metrics.pop();
            for (const auto & item : metric->getInputs())
                metrics.push(item);
            metric->serialize(writer);
        }
        writer.EndArray();
    }
    return result.GetString();
}

}

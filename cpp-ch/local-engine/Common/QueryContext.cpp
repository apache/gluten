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
#include "QueryContext.h"

#include <iomanip>
#include <sstream>
#include <Core/Field.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <IO/WriteHelpers.h>
#include <base/unit.h>
#include <Common/ConcurrentMap.h>
#include <Common/CurrentThread.h>
#include <Common/GlutenConfig.h>
#include <Common/ThreadStatus.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>
#include <Parser/LocalExecutor.h>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace Setting
{
extern const SettingsInt32 os_threads_nice_value_query;
}
}

using namespace DB;

namespace local_engine
{

struct QueryContext::Data
{
    std::shared_ptr<ThreadStatus> thread_status;
    std::shared_ptr<ThreadGroup> thread_group;
    ContextMutablePtr query_context;
    String task_id;

    static DB::ContextMutablePtr global_context;
    static SharedContextHolder shared_context;
};

ContextMutablePtr QueryContext::Data::global_context{};
SharedContextHolder QueryContext::Data::shared_context{};

DB::ContextMutablePtr QueryContext::globalMutableContext()
{
    return Data::global_context;
}

void QueryContext::resetGlobal()
{
    if (Data::global_context)
    {
        Data::global_context->shutdown();
        Data::global_context.reset();
    }
    Data::shared_context.reset();
}

void QueryContext::reset()
{
    {
         std::lock_guard<std::mutex> lock(local_executor_mutex_);
        for (auto it = active_executors_.begin(); it != active_executors_.end(); ++it)
        {
            auto * executor = *it;
            executor->cancel();
            LOG_ERROR(logger_, "Not closed LocalExecutor({}):\n{}", reinterpret_cast<intptr_t>(executor), executor->dumpPipeline());
            delete executor;
        }
        active_executors_.clear();
    }
    query_map_.clear();
    QueryContext::resetGlobal();
}

DB::ContextMutablePtr QueryContext::createGlobal()
{
    assert(Data::shared_context.get() == nullptr);

    if (!Data::shared_context.get())
        Data::shared_context = SharedContextHolder(Context::createShared());

    assert(Data::global_context == nullptr);
    Data::global_context = Context::createGlobal(Data::shared_context.get());
    return globalMutableContext();
}

DB::ContextPtr QueryContext::globalContext()
{
    return Data::global_context;
}

int64_t QueryContext::initializeQuery(const String & task_id)
{
    std::shared_ptr<Data> query_context = std::make_shared<Data>();
    query_context->query_context = Context::createCopy(globalContext());
    query_context->query_context->makeQueryContext();
    query_context->task_id = task_id;

    // empty input will trigger random query id to be set
    // FileCache will check if query id is set to decide whether to skip cache or not
    // check FileCache.isQueryInitialized()
    //
    // Notice:
    // this generated random query id a qualified global queryid for the spark query
    query_context->query_context->setCurrentQueryId(DB::toString(UUIDHelpers::generateV4()) + "_" + task_id);
    auto config = MemoryConfig::loadFromContext(query_context->query_context);
    query_context->thread_status = std::make_shared<ThreadStatus>();
    query_context->thread_group = std::make_shared<ThreadGroup>(query_context->query_context, query_context->query_context->getSettingsRef()[Setting::os_threads_nice_value_query]);
    CurrentThread::attachToGroup(query_context->thread_group);
    auto memory_limit = config.off_heap_per_task;

    query_context->thread_group->memory_tracker.setSoftLimit(memory_limit);
    query_context->thread_group->memory_tracker.setHardLimit(memory_limit + config.extra_memory_hard_limit);
    int64_t id = reinterpret_cast<int64_t>(query_context->thread_group.get());
    query_map_.insert(id, query_context);
    return id;
}

DB::ContextMutablePtr QueryContext::currentQueryContext()
{
    auto thread_group = currentThreadGroup();
    const int64_t id = reinterpret_cast<int64_t>(CurrentThread::getGroup().get());
    return query_map_.get(id)->query_context;
}

std::shared_ptr<DB::ThreadGroup> QueryContext::currentThreadGroup()
{
    if (auto thread_group = CurrentThread::getGroup())
        return thread_group;

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Thread group not found.");
}

String QueryContext::currentTaskIdOrEmpty()
{
    if (auto thread_group = CurrentThread::getGroup())
    {
        const int64_t id = reinterpret_cast<int64_t>(thread_group.get());
        return query_map_.get(id)->task_id;
    }
    return "";
}

void QueryContext::logCurrentPerformanceCounters(ProfileEvents::Counters & counters, const String & task_id) const
{
    if (!CurrentThread::getGroup())
        return;
    if (logger_->information())
    {
        std::ostringstream msg;
        msg << "\n---------------------Task Performance Counters(" << task_id << ")-----------------------------\n";
        for (ProfileEvents::Event event = ProfileEvents::Event(0); event < counters.num_counters; event++)
        {
            const auto name = ProfileEvents::getName(event);
            const auto * doc = ProfileEvents::getDocumentation(event);
            auto & count = counters[event];
            if (count == 0)
                continue;
            msg << std::setw(50) << std::setfill(' ') << std::left << name << "|" << std::setw(20) << std::setfill(' ') << std::left
                << count.load() << " | (" << doc << ")\n";
        }
        LOG_INFO(logger_, "{}", msg.str());
    }
}

size_t QueryContext::currentPeakMemory(int64_t id)
{
    if (!query_map_.contains(id))
        throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "context released {}", id);
    return query_map_.get(id)->thread_group->memory_tracker.getPeak();
}

void QueryContext::finalizeQuery(int64_t id)
{
    if (!CurrentThread::getGroup())
        throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "Thread group not found.");
    std::shared_ptr<Data> context = query_map_.get(id);
    query_map_.erase(id);

    auto query_context = context->thread_status->getQueryContext();
    if (!query_context)
        throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "query context not found");
    context->thread_status->flushUntrackedMemory();
    context->thread_status->finalizePerformanceCounters();
    auto peak = context->thread_group->memory_tracker.getPeak();
    LOG_INFO(logger_, "Task {} finished, peak memory usage: {}", context->task_id, formatReadableSizeWithBinarySuffix(peak));

    auto final_usage = context->thread_group->memory_tracker.get();
    if (final_usage > 2_MiB)
        LOG_WARNING(logger_, "{} memory didn't release, There may be a memory leak!", formatReadableSizeWithBinarySuffix(final_usage));
    logCurrentPerformanceCounters(context->thread_group->performance_counters, context->task_id);
    context->thread_status->detachFromGroup();
    context->thread_group.reset();
    context->thread_status.reset();
    query_context.reset();
}

size_t currentThreadGroupMemoryUsage()
{
    if (!CurrentThread::getGroup())
    {
        LOG_ERROR(getLogger("QueryContext"), "Thread group not found, please call initializeQuery first.");
        return 0;
    }
    return CurrentThread::getGroup()->memory_tracker.get();
}

double currentThreadGroupMemoryUsageRatio()
{
    if (!CurrentThread::getGroup())
    {
        LOG_ERROR(getLogger("QueryContext"), "Thread group not found, please call initializeQuery first.");
        return 0;
    }
    return static_cast<double>(CurrentThread::getGroup()->memory_tracker.get()) / CurrentThread::getGroup()->memory_tracker.getSoftLimit();
}
}

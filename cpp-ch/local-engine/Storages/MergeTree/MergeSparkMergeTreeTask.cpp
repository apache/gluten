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
#include "MergeSparkMergeTreeTask.h"

#include <Interpreters/Context.h>
#include <Interpreters/TransactionLog.h>
#include <Storages/MergeTree/Compaction/CompactionStatistics.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataMergerMutator.h>
#include <Storages/MergeTree/SparkStorageMergeTree.h>
#include <Common/ProfileEvents.h>
#include <Common/ProfileEventsScope.h>
#include <Common/ThreadFuzzer.h>
#include <Common/setThreadName.h>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}
}

using namespace DB;
namespace local_engine
{
StorageID MergeSparkMergeTreeTask::getStorageID() const
{
    return storage.getStorageID();
}

void MergeSparkMergeTreeTask::onCompleted()
{
    bool delay = state == State::SUCCESS;
    task_result_callback(delay);
}

bool MergeSparkMergeTreeTask::executeStep()
{
    /// All metrics will be saved in the thread_group, including all scheduled tasks.
    /// In profile_counters only metrics from this thread will be saved.
    ProfileEventsScope profile_events_scope(&profile_counters);

    /// Make out memory tracker a parent of current thread memory tracker
    std::optional<ThreadGroupSwitcher> switcher;
    if (merge_list_entry)
    {
        switcher.emplace((*merge_list_entry)->thread_group, ThreadName::GLUTEN_MERGE_MERGETREE, /*allow_existing_group*/ true);
    }

    switch (state)
    {
        case State::NEED_PREPARE: {
            prepare();
            state = State::NEED_EXECUTE;
            return true;
        }
        case State::NEED_EXECUTE: {
            try
            {
                if (merge_task->execute())
                    return true;

                state = State::NEED_FINISH;
                return true;
            }
            catch (...)
            {
                write_part_log(ExecutionStatus::fromCurrentException("", true));
                throw;
            }
        }
        case State::NEED_FINISH: {
            finish();

            state = State::SUCCESS;
            return false;
        }
        case State::SUCCESS: {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Task with state SUCCESS mustn't be executed again");
        }
    }
}


void MergeSparkMergeTreeTask::cancel() noexcept
{
    if (merge_task)
        merge_task->cancel();
}

void MergeSparkMergeTreeTask::prepare()
{
    future_part = merge_mutate_entry->future_part;
    stopwatch_ptr = std::make_unique<Stopwatch>();

    task_context = createTaskContext();
    merge_list_entry = storage.getContext()->getMergeList().insert(
        storage.getStorageID(),
        future_part,
        task_context);

    write_part_log = [this](const ExecutionStatus & execution_status)
    {
        auto profile_counters_snapshot = std::make_shared<ProfileEvents::Counters::Snapshot>(profile_counters.getPartiallyAtomicSnapshot());
        merge_task.reset();
        storage.writePartLog(
            PartLogElement::MERGE_PARTS,
            execution_status,
            stopwatch_ptr->elapsed(),
            future_part->name,
            new_part,
            future_part->parts,
            merge_list_entry.get(),
            std::move(profile_counters_snapshot));
    };

    transfer_profile_counters_to_initial_query = [this, query_thread_group = CurrentThread::getGroup()]()
    {
        if (query_thread_group)
        {
            auto task_thread_group = (*merge_list_entry)->thread_group;
            auto task_counters_snapshot = task_thread_group->performance_counters.getPartiallyAtomicSnapshot();

            auto & query_counters = query_thread_group->performance_counters;
            for (ProfileEvents::Event i = ProfileEvents::Event(0); i < ProfileEvents::end(); ++i)
                query_counters.incrementNoTrace(i, task_counters_snapshot[i]);
        }
    };

    merge_task = storage.merger_mutator.mergePartsToTemporaryPart(
        future_part,
        metadata_snapshot,
        merge_list_entry.get(),
        {} /* projection_merge_list_element */,
        table_lock_holder,
        time(nullptr),
        task_context,
        //merge_mutate_entry->tagger->reserved_space,
        storage.tryReserveSpace(
            CompactionStatistics::estimateNeededDiskSpace(future_part->parts), future_part->parts[0]->getDataPartStorage()),
        deduplicate,
        deduplicate_by_columns,
        cleanup,
        storage.merging_params,
        txn,
        // need_prefix = false, so CH won't create a tmp_ folder while merging.
        // the tmp_ folder is problematic when on S3 (particularlly when renaming)
        false);
}


void MergeSparkMergeTreeTask::finish()
{
    new_part = merge_task->getFuture().get();

    // Since there is not tmp_ folder, we don't need renaming
    // MergeTreeData::Transaction transaction(storage, txn.get());
    // storage.merger_mutator.renameMergedTemporaryPart(new_part, future_part->parts, txn, transaction);
    // transaction.commit();
    new_part->getDataPartStoragePtr()->commitTransaction();
    ThreadFuzzer::maybeInjectSleep();
    ThreadFuzzer::maybeInjectMemoryLimitException();

    write_part_log({});
    storage.incrementMergedPartsProfileEvent(new_part->getType());
    transfer_profile_counters_to_initial_query();

    if (auto txn_ = txn_holder.getTransaction())
    {
        /// Explicitly commit the transaction if we own it (it's a background merge, not OPTIMIZE)
        TransactionLog::instance().commitTransaction(txn_, /* throw_on_unknown_status */ false);
        ThreadFuzzer::maybeInjectSleep();
        ThreadFuzzer::maybeInjectMemoryLimitException();
    }

    new_part->is_temp = false;
}

ContextMutablePtr MergeSparkMergeTreeTask::createTaskContext() const
{
    auto context = Context::createCopy(storage.getContext());
    context->makeQueryContext();
    auto queryId = getQueryId();
    context->setCurrentQueryId(queryId);
    return context;
}
}
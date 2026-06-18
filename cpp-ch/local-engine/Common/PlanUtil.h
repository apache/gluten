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
#include <set>
#include <string>

#include <Core/Block.h>
#include <Core/Names.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{
class Block;
class IQueryPlanStep;
class QueryPipeline;
class QueryPlan;
}

namespace local_engine
{
namespace PlanUtil
{
using BuildNamesWithAliases = const std::function<void(const DB::Block &, DB::NamesWithAliases &)>;

std::string explainPlan(const DB::QueryPlan & plan);
void checkOuputType(const DB::QueryPlan & plan);
DB::IQueryPlanStep * adjustQueryPlanHeader(DB::QueryPlan & plan, const DB::Block & to_header, const std::string & step_desc = "", const DB::ContextPtr context = nullptr);
DB::IQueryPlanStep * addRemoveNullableStep(DB::QueryPlan & plan, const DB::ContextPtr & context, const std::set<std::string> & columns);
DB::IQueryPlanStep *
renamePlanHeader(DB::QueryPlan & plan, const BuildNamesWithAliases & buildAliases, const std::string & step_desc = "Rename Output");
}
}
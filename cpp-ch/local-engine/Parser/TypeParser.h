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
#include <list>

#include <unordered_map>
#include <Core/Block.h>
#include <DataTypes/IDataType.h>
#include <substrait/plan.pb.h>

namespace local_engine
{
    class TypeParser
    {
    public:
        TypeParser() = default;
        ~TypeParser() = default;

        static String getCHTypeName(const String& spark_type_name);

        static DB::DataTypePtr getCHTypeByName(const String& spark_type_name);

        /// When parsing named structure, we need the field names.
        static DB::DataTypePtr
        parseType(const substrait::Type& substrait_type, std::list<String>* field_names, bool keep_list_nullability = false);

        inline static DB::DataTypePtr parseType(const substrait::Type& substrait_type)
        {
            return parseType(substrait_type, nullptr);
        }

        // low_card_cols is in format of "cola,colb". Currently does not nested column to be LowCardinality.
        static DB::Block buildBlockFromNamedStruct(const substrait::NamedStruct& struct_, const std::string& low_card_cols = "");

        /// Build block from substrait NamedStruct without DFS rules, different from buildBlockFromNamedStruct
        static DB::Block buildBlockFromNamedStructWithoutDFS(const substrait::NamedStruct& struct_);

        static bool isTypeMatched(const substrait::Type & substrait_type, const DB::DataTypePtr & ch_type, bool ignore_nullability = true);
        static DB::DataTypePtr tryWrapNullable(substrait::Type_Nullability nullable, DB::DataTypePtr nested_type);

    private:
        /// Mapping spark type names to CH type names.
        static std::unordered_map<String, String> type_names_mapping;


    };
}

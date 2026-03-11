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
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnStringHelpers.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeArray.h>
#include <Formats/FormatFactory.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <IO/WriteHelpers.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}
}

namespace local_engine
{
class SparkCastComplexTypesToString : public DB::IFunction
{
public:
    static constexpr auto name = "sparkCastComplexTypesToString";

    static DB::FunctionPtr create(DB::ContextPtr context) { return std::make_shared<SparkCastComplexTypesToString>(context); }

    explicit SparkCastComplexTypesToString(DB::ContextPtr context_) : context(context_) {}

    ~SparkCastComplexTypesToString() override = default;

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    bool isSuitableForShortCircuitArgumentsExecution(const DB::DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    bool useDefaultImplementationForNulls() const override { return false; }

    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

    DB::DataTypePtr getReturnTypeImpl(const DB::ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 1)
            throw DB::Exception(DB::ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} argument size must be 1", name);

        auto arg_type = DB::WhichDataType(DB::removeNullable(arguments[0].type));

        if (!arg_type.isTuple() && !arg_type.isMap() && !arg_type.isArray())
            throw DB::Exception(
                DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument[0] of function {}, should be struct, map or array",
                arguments[0].type->getName(), getName());

        if (arguments[0].type->isNullable())
            return DB::makeNullable(std::make_shared<DB::DataTypeString>());
        else
            return std::make_shared<DB::DataTypeString>();
    }

    DB::ColumnPtr executeImpl(const DB::ColumnsWithTypeAndName & arguments, const DB::DataTypePtr & result_type, size_t input_rows_count) const override
    {
        DB::ColumnUInt8::MutablePtr null_map = nullptr;
        if (const auto * col_nullable = DB::checkAndGetColumn<DB::ColumnNullable>(arguments[0].column.get()))
        {
            null_map = DB::ColumnUInt8::create();
            null_map->insertRangeFrom(col_nullable->getNullMapColumn(), 0, col_nullable->size());
        }

        const auto & nested_col_with_type_and_name = columnGetNested(arguments[0]);

        if (const auto * col_const = DB::checkAndGetColumn<DB::ColumnConst>(nested_col_with_type_and_name.column.get()))
        {
            DB::ColumnsWithTypeAndName new_arguments {1};
            new_arguments[0] = {col_const->getDataColumnPtr(), nested_col_with_type_and_name.type, nested_col_with_type_and_name.name};
            auto col = executeImpl(new_arguments, result_type, 1);
            return DB::ColumnConst::create(std::move(col), input_rows_count);
        }

        DB::FormatSettings format_settings = context ? DB::getFormatSettings(context) : DB::FormatSettings{};
        format_settings.pretty.charset = DB::FormatSettings::Pretty::Charset::ASCII; /// Use ASCII for pretty output.

        auto res_col = removeNullable(result_type)->createColumn();
        DB::ColumnStringHelpers::WriteHelper write_helper(assert_cast<DB::ColumnString &>(*res_col), input_rows_count);
        auto & write_buffer = write_helper.getWriteBuffer();

        // TODO: respect spark.sql.legacy.castComplexTypesToString.enabled
        if (const auto * tuple_col = DB::checkAndGetColumn<DB::ColumnTuple>(nested_col_with_type_and_name.column.get()))
        {
            const auto * tuple_type = DB::checkAndGetDataType<DB::DataTypeTuple>(nested_col_with_type_and_name.type.get());
            for (size_t row = 0; row < input_rows_count; ++row)
            {
                serializeTuple(*tuple_col, row, tuple_type->getElements(), write_buffer, format_settings);
                write_helper.finishRow();
            }
            write_helper.finalize();
        }
        else if (const auto * map_col = DB::checkAndGetColumn<DB::ColumnMap>(nested_col_with_type_and_name.column.get()))
        {
            const auto * map_type = DB::checkAndGetDataType<DB::DataTypeMap>(nested_col_with_type_and_name.type.get());
            const auto & key_type = map_type->getKeyType();
            const auto & value_type = map_type->getValueType();
            for (size_t row = 0; row < input_rows_count; ++row)
            {
                serializeMap(*map_col, row, key_type, value_type, write_buffer, format_settings);
                write_helper.finishRow();
            }
            write_helper.finalize();
        }
        else if (const auto * array_col = DB::checkAndGetColumn<DB::ColumnArray>(nested_col_with_type_and_name.column.get()))
        {
            const auto * array_type = DB::checkAndGetDataType<DB::DataTypeArray>(nested_col_with_type_and_name.type.get());
            for (size_t row = 0; row < input_rows_count; ++row)
            {
                serializeArray(*array_col, row, array_type->getNestedType(), write_buffer, format_settings);
                write_helper.finishRow();
            }
            write_helper.finalize();
        }
        else
            throw DB::Exception(
                DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument[0] of function {}, should be struct, map or array",
                    arguments[0].type->getName(), getName());

        if (result_type->isNullable() && null_map)
            return DB::ColumnNullable::create(std::move(res_col), std::move(null_map));
        return res_col;
    }

private:
    DB::ContextPtr context;

    void serializeTuple(
        const DB::ColumnTuple & tuple_col,
        size_t row_num,
        const DB::DataTypes & elems_type,
        DB::WriteBuffer & ostr,
        const DB::FormatSettings & settings) const
    {
        writeChar('{', ostr);
        for (size_t i = 0; i < tuple_col.tupleSize(); ++i)
        {
            if (i != 0)
                writeString(", ", ostr);

            const auto & elem_type = elems_type[i];
            const auto & elem_column = tuple_col.getColumn(i);

            if (DB::WhichDataType(elem_type).isNullable())
            {
                serializeNullable(
                    assert_cast<const DB::ColumnNullable &>(elem_column),
                    row_num,
                    DB::removeNullable(elem_type),
                    ostr,
                    settings);
            }
            else if (isTuple(elem_type))
            {
                serializeTuple(
                    assert_cast<const DB::ColumnTuple &>(elem_column),
                    row_num,
                    assert_cast<const DB::DataTypeTuple &>(*elem_type).getElements(),
                    ostr,
                    settings);
            }
            else if (isMap(elem_type))
            {
                serializeMap(
                    assert_cast<const DB::ColumnMap &>(elem_column),
                    row_num,
                    assert_cast<const DB::DataTypeMap &>(*elem_type).getKeyType(),
                    assert_cast<const DB::DataTypeMap &>(*elem_type).getValueType(),
                    ostr,
                    settings);
            }
            else if (isArray(elem_type))
            {
                const auto & elem_array_type = assert_cast<const DB::DataTypeArray &>(*elem_type);
                serializeArray(
                    assert_cast<const DB::ColumnArray &>(elem_column),
                    row_num,
                    elem_array_type.getNestedType(),
                    ostr,
                    settings);
            }
            else
                elem_type->getDefaultSerialization()->serializeText(elem_column, row_num, ostr, settings);
        }
        writeChar('}', ostr);
    }

    void serializeMap(
        const DB::ColumnMap & map_col,
        size_t row_num,
        const DB::DataTypePtr & key_type,
        const DB::DataTypePtr & value_type,
        DB::WriteBuffer & ostr,
        const DB::FormatSettings & settings) const
    {
        const auto & nested_array = map_col.getNestedColumn();
        const auto & nested_tuple = map_col.getNestedData();
        const auto & offsets = nested_array.getOffsets();

        const auto & key_column = nested_tuple.getColumn(0);
        const auto & value_column = nested_tuple.getColumn(1);

        const auto & key_serializer = key_type->getDefaultSerialization();
        const auto & value_serializer = value_type->getDefaultSerialization();

        size_t offset = offsets[row_num - 1];
        size_t next_offset = offsets[row_num];

        writeChar('{', ostr);
        for (size_t i = offset; i < next_offset; ++i)
        {
            if (i != offset)
                writeCString(", ", ostr);

            if (DB::WhichDataType(key_type).isNullable())
            {
                serializeNullable(
                    assert_cast<const DB::ColumnNullable &>(key_column),
                    i,
                    DB::removeNullable(key_type),
                    ostr,
                    settings);
            }
            // The key of map cannot be/contain map.
            else if (isArray(key_type))
            {
                serializeArray(
                    assert_cast<const DB::ColumnArray &>(key_column),
                    i,
                    assert_cast<const DB::DataTypeArray &>(*key_type).getNestedType(),
                    ostr,
                    settings);
            }
            else if (isTuple(key_type))
            {
                serializeTuple(
                    assert_cast<const DB::ColumnTuple &>(key_column),
                    i,
                    assert_cast<const DB::DataTypeTuple &>(*key_type).getElements(),
                    ostr,
                    settings);
            }
            else
                key_serializer->serializeText(key_column, i, ostr, settings);

            writeCString(" -> ", ostr);

            if (DB::WhichDataType(value_type).isNullable())
            {
                serializeNullable(
                    assert_cast<const DB::ColumnNullable &>(value_column),
                    i,
                    DB::removeNullable(value_type),
                    ostr,
                    settings);
            }
            else if (isArray(value_type))
            {
                serializeArray(
                    assert_cast<const DB::ColumnArray &>(value_column),
                    i,
                    assert_cast<const DB::DataTypeArray &>(*value_type).getNestedType(),
                    ostr,
                    settings);
            }
            else if (isMap(value_type))
            {
                serializeMap(
                    assert_cast<const DB::ColumnMap &>(value_column),
                    i,
                    assert_cast<const DB::DataTypeMap &>(*value_type).getKeyType(),
                    assert_cast<const DB::DataTypeMap &>(*value_type).getValueType(),
                    ostr,
                    settings);
            }
            else if (isTuple(value_type))
            {
                serializeTuple(
                    assert_cast<const DB::ColumnTuple &>(value_column),
                    i,
                    assert_cast<const DB::DataTypeTuple &>(*value_type).getElements(),
                    ostr,
                    settings);
            }
            else
                value_serializer->serializeText(value_column, i, ostr, settings);
        }
        writeChar('}', ostr);
    }

    void serializeArray(
        const DB::ColumnArray & array_col,
        size_t row_num,
        const DB::DataTypePtr & value_type,
        DB::WriteBuffer & ostr,
        const DB::FormatSettings & settings) const
    {
        const DB::ColumnArray::Offsets & offsets = array_col.getOffsets();
        const auto & nested_column = array_col.getDataPtr();

        size_t offset = offsets[row_num - 1];
        size_t next_offset = offsets[row_num];

        const auto & value_serializer = value_type->getDefaultSerialization();

        writeChar('[', ostr);
        for (size_t i = offset; i < next_offset; ++i)
        {
            if (i != offset)
                writeCString(", ", ostr);

            if (DB::WhichDataType(value_type).isNullable())
            {
                serializeNullable(
                    assert_cast<const DB::ColumnNullable &>(*nested_column),
                    i,
                    DB::removeNullable(value_type),
                    ostr,
                    settings);
            }
            else if (isArray(value_type))
            {
                serializeArray(
                    assert_cast<const DB::ColumnArray &>(*nested_column),
                    i,
                    assert_cast<const DB::DataTypeArray &>(*value_type).getNestedType(),
                    ostr,
                    settings);
            }
            else if (isMap(value_type))
            {
                serializeMap(
                    assert_cast<const DB::ColumnMap &>(*nested_column),
                    i,
                    assert_cast<const DB::DataTypeMap &>(*value_type).getKeyType(),
                    assert_cast<const DB::DataTypeMap &>(*value_type).getValueType(),
                    ostr,
                    settings);
            }
            else if (isTuple(value_type))
            {
                serializeTuple(
                    assert_cast<const DB::ColumnTuple &>(*nested_column),
                    i,
                    assert_cast<const DB::DataTypeTuple &>(*value_type).getElements(),
                    ostr,
                    settings);
            }
            else
                value_serializer->serializeText(*nested_column, i, ostr, settings);
        }
        writeChar(']', ostr);
    }

    void serializeNullable(
        const DB::ColumnNullable & column,
        size_t row_num,
        const DB::DataTypePtr & nested_type,
        DB::WriteBuffer & ostr,
        const DB::FormatSettings & settings) const
    {
        if (column.isNullAt(row_num))
        {
            writeCString("null", ostr);
            return;
        }

        const auto & nested_col = column.getNestedColumn();

        if (isArray(nested_type))
        {
            serializeArray(
                assert_cast<const DB::ColumnArray &>(nested_col),
                row_num,
                assert_cast<const DB::DataTypeArray &>(*nested_type).getNestedType(),
                ostr,
                settings);
        }
        else if (isMap(nested_type))
        {
            serializeMap(
                assert_cast<const DB::ColumnMap &>(nested_col),
                row_num,
                assert_cast<const DB::DataTypeMap &>(*nested_type).getKeyType(),
                assert_cast<const DB::DataTypeMap &>(*nested_type).getValueType(),
                ostr,
                settings);
        }
        else if (isTuple(nested_type))
        {
            serializeTuple(
                assert_cast<const DB::ColumnTuple &>(nested_col),
                row_num,
                assert_cast<const DB::DataTypeTuple &>(*nested_type).getElements(),
                ostr,
                settings);
        }
        else
            nested_type->getDefaultSerialization()->serializeText(nested_col, row_num, ostr, settings);
    }
};

}

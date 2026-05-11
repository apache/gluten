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
#include <Columns/ColumnArray.h>
#include <Columns/ColumnFunction.h>
#include <Columns/ColumnNullable.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFunction.h>
#include <DataTypes/DataTypeNullable.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/castColumn.h>
#include <Common/Exception.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
}
}

namespace local_engine
{
using namespace DB;

class SparkFunctionArrayFold : public IFunction
{
public:
    static constexpr auto name = "sparkArrayFold";
    static FunctionPtr create(ContextPtr /*context*/) { return std::make_shared<SparkFunctionArrayFold>(); }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }

    void getLambdaArgumentTypes(DataTypes & arguments) const override
    {
        if (arguments.size() != 3 && arguments.size() != 4)
            throw Exception(
                ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION,
                "Function {} requires a lambda function, an array, and an initial value, with an optional finish lambda",
                getName());

        const auto * merge_lambda = checkAndGetDataType<DataTypeFunction>(arguments[0].get());
        if (!merge_lambda || merge_lambda->getArgumentTypes().size() != 2)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument of function {} must be a lambda function with 2 arguments, found {} instead.",
                getName(),
                arguments[0]->getName());

        const auto * array_type = checkAndGetDataType<DataTypeArray>(removeNullable(arguments[1]).get());
        if (!array_type)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Second argument of function {} must be an array, found {} instead.",
                getName(),
                arguments[1]->getName());

        DataTypes merge_lambda_args = {arguments[2], array_type->getNestedType()};
        arguments[0] = std::make_shared<DataTypeFunction>(merge_lambda_args, merge_lambda->getReturnType());

        if (arguments.size() == 4)
        {
            const auto * finish_lambda = checkAndGetDataType<DataTypeFunction>(arguments[3].get());
            if (!finish_lambda || finish_lambda->getArgumentTypes().size() != 1)
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Fourth argument of function {} must be a lambda function with 1 argument, found {} instead.",
                    getName(),
                    arguments[3]->getName());

            DataTypes finish_lambda_args = {arguments[2]};
            arguments[3] = std::make_shared<DataTypeFunction>(finish_lambda_args, finish_lambda->getReturnType());
        }
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 3 && arguments.size() != 4)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires 3 or 4 arguments", getName());

        const auto * array_type = checkAndGetDataType<DataTypeArray>(removeNullable(arguments[1].type).get());
        if (!array_type)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Second argument of function {} must be an array, found {} instead.",
                getName(),
                arguments[1].type->getName());

        const auto * merge_lambda = checkAndGetDataType<DataTypeFunction>(arguments[0].type.get());
        if (!merge_lambda)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument of function {} must be a lambda function, found {} instead.",
                getName(),
                arguments[0].type->getName());

        DataTypePtr result_type;
        if (arguments.size() == 3)
        {
            result_type = arguments[2].type;
        }
        else
        {
            const auto * finish_lambda = checkAndGetDataType<DataTypeFunction>(arguments[3].type.get());
            if (!finish_lambda)
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Fourth argument of function {} must be a lambda function, found {} instead.",
                    getName(),
                    arguments[3].type->getName());
            result_type = finish_lambda->getReturnType();
        }

        if (arguments[1].type->isNullable() && !result_type->isNullable())
            result_type = makeNullable(result_type);

        return result_type;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const auto * merge_lambda = checkAndGetColumn<ColumnFunction>(arguments[0].column.get());
        if (!merge_lambda)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "First argument of function {} must be a lambda function column, found {} instead.",
                getName(),
                arguments[0].column->getName());

        ColumnPtr array_column = arguments[1].column->convertToFullColumnIfConst();
        DataTypePtr array_type = arguments[1].type;
        const NullMap * array_null_map = nullptr;
        if (const auto * nullable_array_column = checkAndGetColumn<ColumnNullable>(array_column.get()))
        {
            array_column = nullable_array_column->getNestedColumnPtr();
            array_type = removeNullable(array_type);
            array_null_map = &nullable_array_column->getNullMapData();
        }

        const auto * array_col = checkAndGetColumn<ColumnArray>(array_column.get());
        if (!array_col)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Second argument of function {} must be an array column, found {} instead.",
                getName(),
                array_column->getName());

        ColumnPtr init_column = arguments[2].column->convertToFullColumnIfConst();
        DataTypePtr init_type = arguments[2].type;

        const auto * merge_lambda_type = checkAndGetDataType<DataTypeFunction>(arguments[0].type.get());
        if (!merge_lambda_type || merge_lambda_type->getArgumentTypes().size() != 2)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument of function {} must be a lambda function with 2 arguments, found {} instead.",
                getName(),
                arguments[0].type->getName());

        const auto & merge_lambda_args = merge_lambda_type->getArgumentTypes();
        DataTypePtr acc_type = merge_lambda_args[0];
        DataTypePtr element_type = merge_lambda_args[1];

        if (!init_type->equals(*acc_type))
        {
            auto init_arg = ColumnWithTypeAndName(init_column, init_type, "acc");
            init_column = castColumn(init_arg, acc_type);
            init_type = acc_type;
        }

        const ColumnFunction * finish_lambda = nullptr;
        if (arguments.size() == 4)
        {
            finish_lambda = checkAndGetColumn<ColumnFunction>(arguments[3].column.get());
            if (!finish_lambda)
                throw Exception(
                    ErrorCodes::ILLEGAL_COLUMN,
                    "Fourth argument of function {} must be a lambda function column, found {} instead.",
                    getName(),
                    arguments[3].column->getName());
        }

        const auto & offsets = array_col->getOffsets();
        const auto & nested_data = array_col->getData();
        auto nested_type = assert_cast<const DataTypeArray &>(*removeNullable(array_type)).getNestedType();
        const bool needs_element_cast = !nested_type->equals(*element_type);

        auto to_const_column = [](const MutableColumnPtr & column) -> ColumnPtr {
            const IColumn & column_ref = *column;
            return column_ref.getPtr();
        };
        auto make_single_value_column = [&](const DataTypePtr & type, const Field & value) {
            auto col = type->createColumn();
            col->insert(value);
            return to_const_column(col);
        };

        auto result_column = result_type->createColumn();
        result_column->reserve(input_rows_count);

        Field acc_field;
        size_t previous_offset = 0;
        for (size_t row = 0; row < input_rows_count; ++row)
        {
            if (array_null_map && (*array_null_map)[row])
            {
                result_column->insertDefault();
                continue;
            }

            init_column->get(row, acc_field);
            size_t end_offset = offsets[row];
            for (size_t i = previous_offset; i < end_offset; ++i)
            {
                auto acc_col = make_single_value_column(init_type, acc_field);
                auto element_col_mut = nested_data.cloneEmpty();
                element_col_mut->insertFrom(nested_data, i);
                auto element_col = to_const_column(element_col_mut);
                if (needs_element_cast)
                {
                    auto element_arg = ColumnWithTypeAndName(element_col, nested_type, "element");
                    element_col = castColumn(element_arg, element_type);
                }

                auto lambda_clone = merge_lambda->cloneResized(1);
                auto * lambda_col = typeid_cast<ColumnFunction *>(lambda_clone.get());
                lambda_col->appendArguments(
                    {ColumnWithTypeAndName(std::move(acc_col), init_type, "acc"),
                     ColumnWithTypeAndName(std::move(element_col), element_type, "element")});
                auto merged_col = lambda_col->reduce();
                merged_col.column->get(0, acc_field);
            }

            if (finish_lambda)
            {
                const auto * finish_lambda_type = checkAndGetDataType<DataTypeFunction>(arguments[3].type.get());
                if (!finish_lambda_type || finish_lambda_type->getArgumentTypes().size() != 1)
                    throw Exception(
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Fourth argument of function {} must be a lambda function with 1 argument, found {} instead.",
                        getName(),
                        arguments[3].type->getName());

                auto finish_arg_type = finish_lambda_type->getArgumentTypes().front();
                auto acc_col = make_single_value_column(init_type, acc_field);
                if (!init_type->equals(*finish_arg_type))
                    acc_col = castColumn(ColumnWithTypeAndName(acc_col, init_type, "acc"), finish_arg_type);
                auto lambda_clone = finish_lambda->cloneResized(1);
                auto * lambda_col = typeid_cast<ColumnFunction *>(lambda_clone.get());
                lambda_col->appendArguments({ColumnWithTypeAndName(std::move(acc_col), finish_arg_type, "acc")});
                auto finished_col = lambda_col->reduce();
                Field finished_field;
                finished_col.column->get(0, finished_field);
                result_column->insert(finished_field);
            }
            else
            {
                result_column->insert(acc_field);
            }

            previous_offset = end_offset;
        }

        return result_column;
    }

    String getName() const override { return name; }
};

REGISTER_FUNCTION(SparkArrayFold)
{
    factory.registerFunction<SparkFunctionArrayFold>();
}
}
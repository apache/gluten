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

#include "FilePathGenerator.h"
#include "JsonToProtoConverter.h"

#include "memory/VeloxMemoryManager.h"
#include "substrait/SubstraitToVeloxPlan.h"
#include "substrait/SubstraitToVeloxPlanValidator.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/dwio/common/tests/utils/DataFiles.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/type/Type.h"

using namespace facebook::velox;
using namespace facebook::velox::test;
using namespace facebook::velox::connector::hive;
using namespace facebook::velox::exec;

namespace gluten {
namespace {

void addNestedInputSchema(::substrait::ReadRel* read) {
  read->mutable_common()->mutable_direct();
  auto* schema = read->mutable_base_schema();
  for (const auto* name : {"nested", "mask", "value"}) {
    schema->add_names(name);
  }

  auto* nestedType = schema->mutable_struct_()->add_types()->mutable_struct_();
  nestedType->set_nullability(::substrait::Type_Nullability_NULLABILITY_NULLABLE);
  nestedType->add_names("");
  nestedType->add_names("");
  nestedType->add_types()->mutable_i64()->set_nullability(::substrait::Type_Nullability_NULLABILITY_NULLABLE);
  nestedType->add_types()->mutable_bool_()->set_nullability(::substrait::Type_Nullability_NULLABILITY_NULLABLE);
  schema->mutable_struct_()->add_types()->mutable_bool_()->set_nullability(
      ::substrait::Type_Nullability_NULLABILITY_NULLABLE);
  schema->mutable_struct_()->add_types()->mutable_i64()->set_nullability(
      ::substrait::Type_Nullability_NULLABILITY_NULLABLE);
}

} // namespace

class Substrait2VeloxPlanValidatorTest : public exec::test::HiveConnectorTestBase {
 protected:
  bool validatePlan(std::string file) {
    std::string subPlanPath = FilePathGenerator::getDataFilePath(file);

    ::substrait::Plan substraitPlan;
    JsonToProtoConverter::readFromFile(subPlanPath, substraitPlan);
    return validatePlan(substraitPlan);
  }

  bool validatePlan(::substrait::Plan& plan) {
    auto planValidator = std::make_shared<SubstraitToVeloxPlanValidator>(pool_.get());
    return planValidator->validate(plan);
  }
};

TEST_F(Substrait2VeloxPlanValidatorTest, group) {
  std::string subPlanPath = FilePathGenerator::getDataFilePath("group.json");

  ::substrait::Plan substraitPlan;
  JsonToProtoConverter::readFromFile(subPlanPath, substraitPlan);

  ASSERT_FALSE(validatePlan(substraitPlan));
}

TEST_F(Substrait2VeloxPlanValidatorTest, expandSelectionMustBeTopLevelField) {
  const auto makePlan = [](bool nestedSelection) {
    ::substrait::Plan plan;
    auto* expand = plan.add_relations()->mutable_rel()->mutable_expand();
    expand->mutable_common()->mutable_direct();
    addNestedInputSchema(expand->mutable_input()->mutable_read());

    auto* field = expand->add_fields()
                      ->mutable_switching_field()
                      ->add_duplicates()
                      ->mutable_selection()
                      ->mutable_direct_reference()
                      ->mutable_struct_field();
    field->set_field(nestedSelection ? 0 : 1);
    if (nestedSelection) {
      field->mutable_child()->mutable_struct_field()->set_field(1);
    }
    return plan;
  };

  auto topLevelPlan = makePlan(/*nestedSelection=*/false);
  EXPECT_TRUE(validatePlan(topLevelPlan));

  auto nestedPlan = makePlan(/*nestedSelection=*/true);
  EXPECT_FALSE(validatePlan(nestedPlan));
}

TEST_F(Substrait2VeloxPlanValidatorTest, aggregateMaskMustBeTopLevelField) {
  const auto makePlan = [](bool nestedMask) {
    ::substrait::Plan plan;
    auto* extension = plan.add_extensions()->mutable_extension_function();
    extension->set_function_anchor(1);
    extension->set_name("sum:opt_i64");

    auto* aggregate = plan.add_relations()->mutable_rel()->mutable_aggregate();
    aggregate->mutable_common()->mutable_direct();
    addNestedInputSchema(aggregate->mutable_input()->mutable_read());

    auto* measure = aggregate->add_measures();
    auto* maskField =
        measure->mutable_filter()->mutable_selection()->mutable_direct_reference()->mutable_struct_field();
    maskField->set_field(nestedMask ? 0 : 1);
    if (nestedMask) {
      maskField->mutable_child()->mutable_struct_field()->set_field(1);
    }

    auto* function = measure->mutable_measure();
    function->set_function_reference(1);
    function->set_phase(::substrait::AGGREGATION_PHASE_INITIAL_TO_RESULT);
    function->set_invocation(::substrait::AggregateFunction::AGGREGATION_INVOCATION_ALL);
    function->add_arguments()
        ->mutable_value()
        ->mutable_selection()
        ->mutable_direct_reference()
        ->mutable_struct_field()
        ->set_field(2);
    function->mutable_output_type()->mutable_i64()->set_nullability(::substrait::Type_Nullability_NULLABILITY_NULLABLE);
    return plan;
  };

  auto topLevelPlan = makePlan(/*nestedMask=*/false);
  EXPECT_TRUE(validatePlan(topLevelPlan));

  auto nestedPlan = makePlan(/*nestedMask=*/true);
  EXPECT_FALSE(validatePlan(nestedPlan));
}

TEST_F(Substrait2VeloxPlanValidatorTest, castFromUnknown) {
  const auto validateCast = [&](const RowTypePtr& inputType,
                                const std::function<void(::substrait::Expression*)>& setInput,
                                const std::function<void(::substrait::Type*)>& setToType) {
    ::substrait::Expression expression;
    auto* cast = expression.mutable_cast();
    setInput(cast->mutable_input());
    setToType(cast->mutable_type());

    auto planValidator = std::make_shared<SubstraitToVeloxPlanValidator>(pool_.get());
    return planValidator->validate(expression, inputType, {});
  };

  // A null constant, e.g. Spark's NullType, is expressed as the Nothing type.
  const auto setNullInput = [](::substrait::Expression* input) {
    input->mutable_literal()->mutable_null()->mutable_nothing();
  };
  const auto setNullableI32 = [](::substrait::Type* type) {
    type->mutable_i32()->set_nullability(::substrait::Type_Nullability_NULLABILITY_NULLABLE);
  };

  // Casting from Nothing is allowed for both scalar and complex target types.
  EXPECT_TRUE(validateCast(ROW({}, {}), setNullInput, setNullableI32));

  EXPECT_TRUE(validateCast(ROW({}, {}), setNullInput, [&](::substrait::Type* type) {
    auto* list = type->mutable_list();
    list->set_nullability(::substrait::Type_Nullability_NULLABILITY_NULLABLE);
    setNullableI32(list->mutable_type());
  }));

  EXPECT_TRUE(validateCast(ROW({}, {}), setNullInput, [&](::substrait::Type* type) {
    auto* map = type->mutable_map();
    map->set_nullability(::substrait::Type_Nullability_NULLABILITY_NULLABLE);
    setNullableI32(map->mutable_key());
    setNullableI32(map->mutable_value());
  }));

  EXPECT_TRUE(validateCast(ROW({}, {}), setNullInput, [&](::substrait::Type* type) {
    auto* structType = type->mutable_struct_();
    structType->set_nullability(::substrait::Type_Nullability_NULLABILITY_NULLABLE);
    structType->add_names("");
    setNullableI32(structType->add_types());
  }));

  // Casting a complex type to an unrelated type is still not allowed.
  EXPECT_FALSE(validateCast(
      ROW({"a"}, {ARRAY(INTEGER())}),
      [](::substrait::Expression* input) {
        input->mutable_selection()->mutable_direct_reference()->mutable_struct_field()->set_field(0);
      },
      setNullableI32));
}

} // namespace gluten

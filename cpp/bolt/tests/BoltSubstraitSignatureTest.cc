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

#include "substrait/BoltSubstraitSignature.h"
#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/functions/prestosql/registration/RegistrationFunctions.h"

using namespace bytedance::bolt;

namespace gluten {

class BoltSubstraitSignatureTest : public ::testing::Test {
 protected:
  void SetUp() override {
    Test::SetUp();
    functions::prestosql::registerAllScalarFunctions();
  }

  static TypePtr fromSubstraitSignature(const std::string& signature) {
    return BoltSubstraitSignature::fromSubstraitSignature(signature);
  }

  static std::string toSubstraitSignature(const TypePtr& type) {
    return BoltSubstraitSignature::toSubstraitSignature(type);
  }

  static std::string toSubstraitSignature(const std::string& functionName, const std::vector<TypePtr>& arguments) {
    return BoltSubstraitSignature::toSubstraitSignature(functionName, arguments);
  }
};

TEST_F(BoltSubstraitSignatureTest, toSubstraitSignatureWithType) {
  ASSERT_EQ(toSubstraitSignature(BOOLEAN()), "bool");

  ASSERT_EQ(toSubstraitSignature(TINYINT()), "i8");
  ASSERT_EQ(toSubstraitSignature(SMALLINT()), "i16");
  ASSERT_EQ(toSubstraitSignature(INTEGER()), "i32");
  ASSERT_EQ(toSubstraitSignature(BIGINT()), "i64");
  ASSERT_EQ(toSubstraitSignature(REAL()), "fp32");
  ASSERT_EQ(toSubstraitSignature(DOUBLE()), "fp64");
  ASSERT_EQ(toSubstraitSignature(VARCHAR()), "str");
  ASSERT_EQ(toSubstraitSignature(VARBINARY()), "vbin");
  ASSERT_EQ(toSubstraitSignature(TIMESTAMP()), "ts");
  ASSERT_EQ(toSubstraitSignature(DATE()), "date");
  ASSERT_EQ(toSubstraitSignature(ARRAY(BOOLEAN())), "list");
  ASSERT_EQ(toSubstraitSignature(ARRAY(INTEGER())), "list");
  ASSERT_EQ(toSubstraitSignature(MAP(INTEGER(), BIGINT())), "map");
  ASSERT_EQ(toSubstraitSignature(ROW({INTEGER(), BIGINT()})), "struct<i32,i64>");
  ASSERT_EQ(toSubstraitSignature(ROW({ARRAY(INTEGER())})), "struct<list>");
  ASSERT_EQ(toSubstraitSignature(ROW({MAP(INTEGER(), INTEGER())})), "struct<map>");
  ASSERT_EQ(toSubstraitSignature(ROW({ROW({INTEGER()})})), "struct<struct<i32>>");
  ASSERT_EQ(toSubstraitSignature(UNKNOWN()), "u!name");
}

TEST_F(BoltSubstraitSignatureTest, toSubstraitSignatureWithFunctionNameAndArguments) {
  ASSERT_EQ(toSubstraitSignature("eq", {INTEGER(), INTEGER()}), "eq:i32_i32");
  ASSERT_EQ(toSubstraitSignature("gt", {INTEGER(), INTEGER()}), "gt:i32_i32");
  ASSERT_EQ(toSubstraitSignature("lt", {INTEGER(), INTEGER()}), "lt:i32_i32");
  ASSERT_EQ(toSubstraitSignature("gte", {INTEGER(), INTEGER()}), "gte:i32_i32");
  ASSERT_EQ(toSubstraitSignature("lte", {INTEGER(), INTEGER()}), "lte:i32_i32");

  ASSERT_EQ(toSubstraitSignature("and", {BOOLEAN(), BOOLEAN()}), "and:bool_bool");
  ASSERT_EQ(toSubstraitSignature("or", {BOOLEAN(), BOOLEAN()}), "or:bool_bool");
  ASSERT_EQ(toSubstraitSignature("not", {BOOLEAN()}), "not:bool");
  ASSERT_EQ(toSubstraitSignature("xor", {BOOLEAN(), BOOLEAN()}), "xor:bool_bool");

  ASSERT_EQ(toSubstraitSignature("between", {INTEGER(), INTEGER(), INTEGER()}), "between:i32_i32_i32");

  ASSERT_EQ(toSubstraitSignature("plus", {INTEGER(), INTEGER()}), "plus:i32_i32");
  ASSERT_EQ(toSubstraitSignature("divide", {INTEGER(), INTEGER()}), "divide:i32_i32");

  ASSERT_EQ(toSubstraitSignature("cardinality", {ARRAY(INTEGER())}), "cardinality:list");
  ASSERT_EQ(toSubstraitSignature("array_sum", {ARRAY(INTEGER())}), "array_sum:list");

  ASSERT_EQ(toSubstraitSignature("sum", {INTEGER()}), "sum:i32");
  ASSERT_EQ(toSubstraitSignature("avg", {INTEGER()}), "avg:i32");
  ASSERT_EQ(toSubstraitSignature("count", {INTEGER()}), "count:i32");

  auto functionType = std::make_shared<const FunctionType>(std::vector<TypePtr>{INTEGER(), VARCHAR()}, BIGINT());
  std::vector<TypePtr> types = {MAP(INTEGER(), VARCHAR()), functionType};
  ASSERT_ANY_THROW(toSubstraitSignature("transform_keys", std::move(types)));
}

TEST_F(BoltSubstraitSignatureTest, fromSubstraitSignature) {
  ASSERT_EQ(fromSubstraitSignature("bool")->kind(), TypeKind::BOOLEAN);
  ASSERT_EQ(fromSubstraitSignature("i8")->kind(), TypeKind::TINYINT);
  ASSERT_EQ(fromSubstraitSignature("i16")->kind(), TypeKind::SMALLINT);
  ASSERT_EQ(fromSubstraitSignature("i32")->kind(), TypeKind::INTEGER);
  ASSERT_EQ(fromSubstraitSignature("i64")->kind(), TypeKind::BIGINT);
  ASSERT_EQ(fromSubstraitSignature("fp32")->kind(), TypeKind::REAL);
  ASSERT_EQ(fromSubstraitSignature("fp64")->kind(), TypeKind::DOUBLE);
  ASSERT_EQ(fromSubstraitSignature("str")->kind(), TypeKind::VARCHAR);
  ASSERT_EQ(fromSubstraitSignature("vbin")->kind(), TypeKind::VARBINARY);
  ASSERT_EQ(fromSubstraitSignature("ts")->kind(), TypeKind::TIMESTAMP);
  ASSERT_EQ(fromSubstraitSignature("date")->kind(), TypeKind::INTEGER);
  ASSERT_EQ(fromSubstraitSignature("dec<18,2>")->kind(), TypeKind::BIGINT);
  ASSERT_EQ(fromSubstraitSignature("dec<19,2>")->kind(), TypeKind::HUGEINT);

  // Struct type test.
  auto type = fromSubstraitSignature("struct<bool,vbin,dec<8,2>>");
  ASSERT_EQ(type->kind(), TypeKind::ROW);
  ASSERT_EQ(type->childAt(0)->kind(), TypeKind::BOOLEAN);
  ASSERT_EQ(type->childAt(1)->kind(), TypeKind::VARBINARY);
  ASSERT_EQ(type->childAt(2)->kind(), TypeKind::BIGINT);
  type = fromSubstraitSignature("struct<bool,struct<i8,fp32>>");
  ASSERT_EQ(type->childAt(1)->kind(), TypeKind::ROW);
  ASSERT_EQ(type->childAt(1)->childAt(0)->kind(), TypeKind::TINYINT);
  ASSERT_EQ(type->childAt(1)->childAt(1)->kind(), TypeKind::REAL);
  type = fromSubstraitSignature("struct<bool,struct<i8,fp32>,vbin,ts,dec<9,2>>");
  ASSERT_EQ(type->childAt(1)->kind(), TypeKind::ROW);
  type = fromSubstraitSignature("struct<struct<ts>,i16>");
  ASSERT_EQ(type->childAt(0)->kind(), TypeKind::ROW);
  ASSERT_EQ(type->childAt(0)->childAt(0)->kind(), TypeKind::TIMESTAMP);
  type = fromSubstraitSignature("struct<struct<dec<12,9>>>");
  ASSERT_EQ(type->childAt(0)->kind(), TypeKind::ROW);
  ASSERT_EQ(type->childAt(0)->childAt(0)->kind(), TypeKind::BIGINT);
  type = fromSubstraitSignature("struct<struct<struct<i8>>>");
  ASSERT_EQ(type->kind(), TypeKind::ROW);
  ASSERT_EQ(type->childAt(0)->kind(), TypeKind::ROW);
  ASSERT_EQ(type->childAt(0)->childAt(0)->kind(), TypeKind::ROW);
  ASSERT_EQ(type->childAt(0)->childAt(0)->childAt(0)->kind(), TypeKind::TINYINT);
  type = fromSubstraitSignature("struct<struct<struct<i8,str>>>");
  ASSERT_EQ(type->childAt(0)->childAt(0)->childAt(0)->kind(), TypeKind::TINYINT);
  ASSERT_EQ(type->childAt(0)->childAt(0)->childAt(1)->kind(), TypeKind::VARCHAR);
  type = fromSubstraitSignature("struct<struct<struct<i8,dec<19,2>>>>");
  ASSERT_EQ(type->childAt(0)->childAt(0)->childAt(1)->kind(), TypeKind::HUGEINT);
  type = fromSubstraitSignature("struct<fp32,struct<struct<i8,dec<19,2>,list<i32>,map<i16,i32>>>>");
  ASSERT_EQ(type->childAt(0)->kind(), TypeKind::REAL);
  ASSERT_EQ(type->childAt(1)->childAt(0)->childAt(0)->kind(), TypeKind::TINYINT);
  ASSERT_EQ(type->childAt(1)->childAt(0)->childAt(1)->kind(), TypeKind::HUGEINT);
  ASSERT_EQ(type->childAt(1)->childAt(0)->childAt(2)->childAt(0)->kind(), TypeKind::INTEGER);
  ASSERT_EQ(type->childAt(1)->childAt(0)->childAt(3)->childAt(0)->kind(), TypeKind::SMALLINT);
  ASSERT_EQ(type->childAt(1)->childAt(0)->childAt(3)->childAt(1)->kind(), TypeKind::INTEGER);
  ASSERT_ANY_THROW(fromSubstraitSignature("other")->kind());

  // Map type test.
  type = fromSubstraitSignature("map<bool,list<map<str,i32>>>");
  ASSERT_EQ(type->kind(), TypeKind::MAP);
  ASSERT_EQ(type->childAt(0)->kind(), TypeKind::BOOLEAN);
  ASSERT_EQ(type->childAt(1)->kind(), TypeKind::ARRAY);
  ASSERT_EQ(type->childAt(1)->childAt(0)->kind(), TypeKind::MAP);
  type = fromSubstraitSignature("struct<map<bool,i8>,list<map<str,i32>>>");
  ASSERT_EQ(type->kind(), TypeKind::ROW);
  ASSERT_EQ(type->childAt(0)->kind(), TypeKind::MAP);
  ASSERT_EQ(type->childAt(0)->childAt(0)->kind(), TypeKind::BOOLEAN);
  ASSERT_EQ(type->childAt(0)->childAt(1)->kind(), TypeKind::TINYINT);
  ASSERT_EQ(type->childAt(1)->kind(), TypeKind::ARRAY);
  ASSERT_EQ(type->childAt(1)->childAt(0)->kind(), TypeKind::MAP);
  ASSERT_EQ(type->childAt(1)->childAt(0)->childAt(0)->kind(), TypeKind::VARCHAR);
  ASSERT_EQ(type->childAt(1)->childAt(0)->childAt(1)->kind(), TypeKind::INTEGER);
}

} // namespace gluten

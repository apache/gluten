---
layout: page
title: Bolt Function Development
nav_order: 19
parent: Developer Overview
---

# Developer Guide for Implementing Spark SQL Functions in Bolt

This guide provides instructions for implementing and registering Spark SQL's built-in functions within the Bolt native engine for use with Gluten. Understanding Bolt's function architecture and its interaction with Gluten is key to extending its capabilities.

## High-Level Support and Naming Conventions

Bolt maintains two primary namespaces for functions, located in `bolt/functions/`:

1.  **`prestosql`**: Contains a rich library of functions originally implemented with Presto-compatible semantics. These are registered by default.
2.  **`sparksql`**: Contains functions specifically implemented or overridden to match Spark SQL's semantics.

When Gluten initializes the Bolt backend, it first registers all functions from the `prestosql` namespace. Afterward, it registers functions from the `sparksql` namespace. This layering mechanism allows `sparksql` functions with the same name and signature to transparently override their `prestosql` counterparts. This is crucial for ensuring semantic compatibility with Spark.

If a required function is missing from both namespaces, or if a `prestosql` function's behavior deviates from Spark's (even with a matching signature), a new implementation must be added to the `sparksql` folder.

**Gluten to Bolt Function Mapping:** Gluten translates Spark's `Expression` nodes into a Substrait plan. Bolt then consumes this plan, mapping Substrait function calls to its registered native functions. The function name used in Spark SQL is converted to a lowercase string (e.g., `BitwiseAnd` becomes `bitwise_and`) which is used to look up the corresponding native implementation in Bolt's function registry.

### Concrete Examples

*   **Scalar Function**: The Spark SQL function `hash(col1, col2, ...)` is mapped to the `hash` function in Bolt, which has a corresponding C++ implementation in `bolt/functions/sparksql/Hash.cpp`.
*   **Aggregate Function**: The `SUM()` function in Spark is mapped to the `sum` aggregate function in Bolt. Its implementation, `SumAggregate`, can be found in `bolt/functions/sparksql/aggregates/SumAggregate.cpp`. This implementation handles various numeric types and demonstrates how partial and final aggregations are managed.

## Anatomy of a Bolt Function

At its core, a Bolt function is a C++ struct that implements a specific set of methods and follows certain conventions.

### Scalar Functions (Simple Functions)

Simple functions in Bolt operate on one row at a time. They are defined within a struct, typically templated to handle different data types.

Key components:

*   **`BOLT_DEFINE_FUNCTION_TYPES(T)`**: A macro required inside the function struct to define necessary type aliases like `out_type<Varchar>` and `arg_type<Varchar>`.
*   **`call()` Method**: The main execution logic for non-nullable inputs. The first argument is always a non-const reference to the output (`result`), followed by const references to the inputs. A `void` return type signifies that the function never produces a null output for non-null inputs.
*   **`callNullable()` Method**: If a function needs to handle `NULL` inputs explicitly (i.e., a null input doesn't automatically mean a null output), this method should be implemented. Input arguments are passed as pointers, which will be `nullptr` if the input is null. A `bool` return from `call()` or `callNullable()` controls the nullability of the result (`true` for not-null, `false` for null).
*   **`is_deterministic` Flag**: A static `constexpr bool` that can be set to `false` if the function is non-deterministic (e.g., `rand()`). By default, functions are considered deterministic.

**Example: A simple `plus` function**

```cpp
// From bolt/examples/SimpleFunctions.cpp
template <typename TExecParams>
struct MyPlusFunction {
  FOLLY_ALWAYS_INLINE void
  call(int64_t& out, const int64_t& a, const int64_t& b) {
    out = a + b;
  }
};
```

### Aggregate Functions

Aggregate functions are more complex and typically inherit from `bolt::exec::Aggregate`. They require methods to manage the accumulation state across rows.

*   **Accumulator (`char*`)**: A raw memory buffer to hold the intermediate aggregation state. Its layout is defined by the function.
*   **`initializeNewGroups()`**: Initializes the accumulators for a new set of groups.
*   **`addRawInput()` / `addIntermediateResults()` / `addSingleGroupRawInput()`**: These methods define the core logic for accumulating data from raw input rows or partial aggregation results.
*   **`extractValues()` / `extractRawValues()`**: Extracts the final or intermediate result from the accumulators and writes it to the output vector.
*   **`destroy()`**: Cleans up any resources held by the accumulators (e.g., complex objects).

### Window Functions

Window functions are defined by inheriting from `bolt::exec::WindowFunction` and implementing the following methods:
*   `resetPartition()`: Called when the engine starts processing a new partition.
*   `apply()`: Called for a batch of rows within a partition to compute the function's output. It receives frame and peer boundaries.

## Function Registration

### Scalar Function Registration

Scalar functions are registered using `registerFunction` or helper templates from `RegistrationHelpers.h`. Registration specifies the function's name, its C++ implementation struct, and its signature (return type followed by argument types).

```cpp
// From bolt/examples/SimpleFunctions.cpp

// Register a specific signature
registerFunction<MyPlusFunction, int64_t, int64_t, int64_t>({"my_plus"});

// Use helpers for common patterns (e.g., all numeric types)
functions::registerBinaryNumeric<MyPlusTemplatedFunction>({"my_other_plus"});
```

All Spark SQL functions should be registered in `bolt/functions/sparksql/registration/Register.cpp` by adding a call to a category-specific registration function (e.g., `registerStringFunctions(prefix)`).

### Aggregate Function Registration

Aggregate functions are registered via `registerAggregateFunction` in `bolt/functions/sparksql/aggregates/Register.cpp`. This associates a function name with its implementation class and a set of supported signatures.

```cpp
// From bolt/functions/sparksql/aggregates/SumAggregate.cpp
exec::registerAggregateFunction(
    name,
    std::move(signatures),
    [name](
        core::AggregationNode::Step step,
        const std::vector<TypePtr>& argTypes,
        const TypePtr& resultType,
        const core::QueryConfig& config) -> std::unique_ptr<exec::Aggregate> {
      // Factory lambda that creates an instance of the aggregate function
      // based on input/output types and aggregation step (partial, final, etc.).
      ...
      return std::make_unique<SumAggregate<...>>(...);
    },
    ...
);
```

### Window Function Registration

Window functions are registered using `registerWindowFunction` in `bolt/functions/sparksql/window/WindowFunctionsRegistration.cpp`.

```cpp
// From bolt/functions/sparksql/window/WindowFunctionsRegistration.cpp
void registerWindowFunctions(const std::string& prefix) {
  functions::window::registerNthValueInteger(prefix + "nth_value");
  functions::window::registerRowNumberInteger(prefix + "row_number");
  // ... and so on
}
```

## Semantics and Considerations

### Null Handling

*   **Default Behavior**: If any input to a simple function is `NULL`, the framework automatically produces a `NULL` output without calling the function.
*   **Custom Behavior**: To override this, implement `callNullable(result, arg1, arg2, ...)`. Arguments are passed as pointers, which will be `nullptr` for `NULL` values. This is useful for functions like `coalesce` or `is_null`.

### ANSI SQL Mode

**Gluten does not currently support Spark's ANSI mode.** When `spark.sql.ansi.enabled` is true, Gluten will typically fall back to vanilla Spark for execution. Therefore, function implementations in Bolt only need to consider the behavior of ANSI mode being **off**. This is particularly relevant for functions like `cast`, where error handling for invalid conversions (e.g., string to int) should not throw an exception but return `NULL` instead.

### Determinism

By default, all functions are assumed to be deterministic. If your function can produce different results for the same input (e.g., `rand()`, `now()`), you must declare it as non-deterministic:

```cpp
template <typename T>
struct MyNonDeterministicFunction {
  static constexpr bool is_deterministic = false;
  // ... implementation ...
};
```

### Error Handling

For recoverable errors (e.g., invalid input format for a function), Bolt functions should use `BOLT_USER_FAIL` or `BOLT_USER_CHECK`. This mechanism allows Bolt to propagate a well-defined error back to Gluten and Spark, rather than crashing the process. Unrecoverable logic errors should still use `BOLT_CHECK` or `BOLT_FAIL`.

```cpp
BOLT_USER_CHECK(
    isValid(input), "Invalid input format for my_function: {}", input);
```

## Performance and Memory

*   **Vectorization**: While simple functions operate row-by-row conceptually, the framework ensures they are executed efficiently over columnar data. Avoid branching or complex logic inside the `call` method.
*   **Memory Pool**: Functions have access to a `memory::MemoryPool` for allocations. For string results, the `StringWriter` (`out_type<Varchar>`) handles buffer management automatically.
*   **String Optimizations**:
    *   **`reuse_strings_from_arg`**: For functions that return a substring of an input (e.g., `substr`, `split`), setting `static constexpr int32_t reuse_strings_from_arg = <arg_index>;` allows the output to be a zero-copy view over the input's buffer, significantly reducing memory traffic.
    *   **ASCII Fast Path**: If a string function can be implemented more efficiently for pure-ASCII inputs, provide a `callAscii()` method. The engine will automatically detect ASCII-only input vectors and dispatch to this optimized path.

## Testing

New functions must be accompanied by tests.
*   **Location**: Unit tests for a new function, say `my_function`, should be placed in a corresponding new file, e.g., `bolt/functions/sparksql/tests/MyFunctionTest.cpp`.
*   **Framework**: Tests typically inherit from `SparkFunctionBaseTest` and use `evaluateOnce` or `evaluate` to invoke the function with test data and verify the results.
*   **Cross-Validation**: It is critical to ensure the Bolt implementation matches Spark's behavior. The most effective way to do this is to add integrated tests within the Gluten repository that run the same Spark SQL query against both vanilla Spark and the Gluten-Bolt backend, then compare the results.

## Putting It All Together: A Complete Example

Here is a minimal example of adding a new scalar function `my_scalar_add(a, b)` that adds two integers.

1.  **Implement the function in `bolt/functions/sparksql/MyScalarAdd.h` (new file):**

    ```cpp
    #pragma once

    #include "bolt/functions/Udf.h"

    namespace bytedance::bolt::functions::sparksql {

    template <typename T>
    struct MyScalarAddFunction {
      BOLT_DEFINE_FUNCTION_TYPES(T);

      FOLLY_ALWAYS_INLINE void call(
          int64_t& result,
          const int64_t& a,
          const int64_t& b) {
        result = a + b;
      }
    };
    } // namespace bytedance::bolt::functions::sparksql
    ```

2.  **Register the function in `bolt/functions/sparksql/registration/RegisterMisc.cpp` (or a new registration file):**

    ```cpp
    #include "bolt/functions/sparksql/MyScalarAdd.h"
    #include "bolt/functions/lib/RegistrationHelpers.h"

    namespace bytedance::bolt::functions::sparksql {

    void registerMyFunctions(const std::string& prefix) {
      registerFunction<MyScalarAddFunction, int64_t, int64_t, int64_t>(
          {prefix + "my_scalar_add"});
    }

    // In registerMiscFunctions (or a new top-level registration function)
    void registerMiscFunctions(const std::string& prefix) {
        // ... other registrations
        registerMyFunctions(prefix);
    }
    }
    ```

3.  **Add a test in `bolt/functions/sparksql/tests/MyScalarAddTest.cpp`:**

    ```cpp
    #include "bolt/functions/sparksql/tests/SparkFunctionBaseTest.h"

    using namespace bytedance::bolt;
    using namespace bytedance::bolt::test;

    class MyScalarAddTest : public functions::test::SparkFunctionBaseTest {
    protected:
      void testAdd(int64_t a, int64_t b, int64_t expected) {
        auto result = evaluateOnce<int64_t>(
            "my_scalar_add(c0, c1)",
            makeRowVector({
                makeConstant((int64_t)a, 1),
                makeConstant((int64_t)b, 1),
            }));
        ASSERT_EQ(result.value(), expected);
      }
    };

    TEST_F(MyScalarAddTest, basic) {
      testAdd(10, 20, 30);
      testAdd(-5, 5, 0);
    }
    ```
This structured approach ensures that new functions are correctly implemented, registered, and tested, maintaining compatibility with Spark SQL and high performance within the Bolt engine.

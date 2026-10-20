---
layout: page
title: Velox Function Development
nav_order: 5
parent: Developer Overview
---
# Developer Guide for Implementing Spark Built-in SQL Functions in Velox

In velox, two folders `prestosql` & `sparksql` are holding most sql functions, respective for `presto` and `spark`. Gluten will ask velox to firstly register `prestosql` functions, then `sparksql` functions. So if `prestosql`
and `sparksql` share same signature for a function, the `sparksql` function will overwrite the corresponding `prestosql` function. If the required function is lacking in both folders (exceptions are some common functions defined
outside, like `cast`), we need to implement the missing function in `sparksql` folder. It is possible that a `prestosql` function has some semantic difference with the corresponding spark function, even though they share the
same name and function signature. If so, we also need to do an implementation in `sparksql` folder, generally based on the original impl. for `prestosql`.

There are a few spark functions that can behave differently for some special cases, depending on ANSI on or off. Currently, gluten does NOT support ANSI mode. So only ANSI off needs to be considered in implementing spark
built-in functions in velox.

Take `BitwiseAndFunction` as example:

```
template <typename T>
struct BitwiseAndFunction {
  template <typename TInput>
  // For void return type, it indicates null result will never be obtained for non-null input.
  // For bool return type, it indicates null result can be obtained for non-null input (false for null).
  FOLLY_ALWAYS_INLINE void call(TInput& result, TInput a, TInput b) {
    result = a & b;
  }
};
``` 
It is templated, as well as the `call` function, to allow multiple types. In the above impl., the result will be null for null input.
Please use `callNullable` if you need different behavior for null input, e.g., get a non-null result for null input. Also see `callNullFree` in velox document.
It is used for fast evaluation in the case that any input has null.

The below code will register the implemented function for all kinds of integer types. The specified name `bitwise_and` will be actually used in calling this function.
```
registerBinaryIntegral<BitwiseAndFunction>({prefix + "bitwise_and"});
```

Functions for complex types have similar implementations. 
See `ArrayAverageFunction` in [velox/functions/prestosql/ArrayFunctions.h](https://github.com/facebookincubator/velox/blob/main/velox/functions/prestosql/ArrayFunctions.h).

## Gluten Function Overlay

Upstreaming a function to Velox can take a long time. To avoid being blocked on that, Gluten provides a function overlay in
[cpp/velox/operators/functions/overlay](https://github.com/apache/gluten/tree/main/cpp/velox/operators/functions/overlay),
where Velox-compatible function implementations can be hosted and managed on the Gluten side. The overlay is registered by
`gluten::registerFunctionOverlay()` at the end of `gluten::registerAllFunctions()`, after all Velox presto/spark functions.
Since a later registration with the same name and signature replaces the earlier one in Velox's registries, an overlay function
takes precedence over the Velox implementation.

Use the overlay when:
* A Spark function is missing in Velox. Implement it in the overlay first so Gluten can offload it immediately, then upstream it
  to Velox at your own pace.
* A Velox `sparksql` function has a semantic gap with Spark. Put the corrected implementation in the overlay to override it while
  the fix is pending upstream. The `round` function (`overlay/Round.h`) is an example of such an override.

To add a function:
1. Implement it in a file under `cpp/velox/operators/functions/overlay/`, using Velox's function authoring APIs (simple function,
   vector function, aggregate, or window function).
2. Register it in `overlay/RegisterFunctionOverlay.cc`, using the function name Gluten's Substrait plan conversion emits.
3. If a new `.cc` file is added, list it in `cpp/velox/CMakeLists.txt`.
4. Add C++ unit tests in `cpp/velox/tests/SparkFunctionTest.cc` and Scala side query tests where applicable.
5. For a brand-new function, also add the Scala side expression mapping (`ExpressionNames.scala` / `ExpressionMappings.scala`)
   so the planner offloads it.

The overlay is a staging area, not a permanent fork: once a function is accepted into upstream Velox, remove it from the overlay
in the same PR that bumps the Velox version.

### Reference:
Velox’s official developer guide:
  * [velox/docs/develop/scalar-functions.rst](https://github.com/facebookincubator/velox/blob/main/velox/docs/develop/scalar-functions.rst)
  * [velox/examples/SimpleFunctions.cpp](https://github.com/facebookincubator/velox/blob/main/velox/examples/SimpleFunctions.cpp)

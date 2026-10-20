# Gluten Function Overlay

This directory is Gluten's function overlay: a place to host Velox-compatible
function implementations that are managed on the Gluten side, so a new or fixed
function does not have to be upstreamed to Velox before it can ship in Gluten.

Typical use cases:

* A Spark function that is missing in Velox. Implement it here first, so Gluten
  can offload it right away, and upstream it to Velox later.
* A Velox `sparksql` function whose semantics diverge from Spark. Implement the
  corrected version here to override the Velox one while the fix is pending
  upstream.

## How it works

`registerFunctionOverlay()` (see `RegisterFunctionOverlay.h`) is called at the
end of `gluten::registerAllFunctions()`, after all Velox presto/spark function
registrations. Velox's function registries let a later registration with the
same name and signature replace an earlier one, so any function registered in
the overlay takes precedence over the Velox implementation.

## Adding a function

1. Implement the function in a header/source file in this directory, following
   Velox's function authoring APIs (simple function, vector function, aggregate,
   or window function). See `Round.h` for a simple-function example and
   [Velox scalar functions guide](https://github.com/facebookincubator/velox/blob/main/velox/docs/develop/scalar-functions.rst).
2. Register it in `RegisterFunctionOverlay.cc` inside
   `registerFunctionOverlay()`. Use the same name Gluten's Substrait plan
   conversion emits (Spark's function name in most cases).
3. If you added a new `.cc` file, add it to the source list in
   `cpp/velox/CMakeLists.txt`.
4. Add unit tests in `cpp/velox/tests/SparkFunctionTest.cc` (function overlay is
   registered there through `registerAllFunctions()`), and Scala side tests
   where applicable.
5. If the function is brand new (not just an override), make sure it is mapped
   on the Scala side (e.g. expression mappings in
   `ExpressionMappings.scala` / `ExpressionNames`) so the planner offloads it.

## Lifecycle

The overlay is a staging area, not a permanent fork. When an overlay function
is accepted into upstream Velox, remove it from the overlay in the same PR that
bumps the Velox version, so the Velox implementation takes effect.

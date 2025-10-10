---
layout: page
title: Bolt Backend Features
nav_order: 25
---

# Bolt Backend Features

The Bolt backend integrates Gluten with ByteDance Bolt, a native execution
engine for analytical workloads. Bolt has been developed with production Spark
workloads in mind, including concerns such as native execution performance and
runtime stability.

The goal of the Gluten Bolt backend is to make those capabilities available
through Gluten's backend abstraction. Users can evaluate Bolt through the same
Spark plugin model while using Gluten documentation for build, configuration,
compatibility, migration, and validation steps.

This page is a navigation entry point. It does not duplicate Bolt's feature
articles, redefine Bolt feature categories, or turn discussion notes into formal
performance claims.

## Feature Overview

Bolt feature and optimization details are maintained by the Bolt project. This
page intentionally does not duplicate those articles or redefine Bolt feature
categories in Gluten documentation.

For the latest detailed discussion of Bolt-specific features and optimizations,
refer to the Bolt documentation:

- [Bolt blog](https://bytedance.github.io/bolt/blog/)
- [Bolt blog source posts](https://github.com/bytedance/bolt/tree/main/doc/blog/_posts)

## Gluten Integration References

The following Gluten documents cover how to build, enable, configure, and
validate the Bolt backend from the Spark side.

| Topic | Gluten documentation |
|-------|----------------------|
| Build and smoke test | [Bolt Quick Start](bolt-quick-start.md) |
| Migration from Velox | [Velox to Bolt Migration Guide](velox-to-bolt-migration-guide.md) |
| Backend configuration | [Bolt Backend Configuration](bolt-configuration.md) |
| Spark configuration compatibility | [Bolt Spark Configuration](bolt-spark-configuration.md) |
| Function compatibility | [Scalar](bolt-backend-scalar-function-support.md), [Aggregate](bolt-backend-aggregate-function-support.md), [Window](bolt-backend-window-function-support.md), [Generator](bolt-backend-generator-function-support.md) |

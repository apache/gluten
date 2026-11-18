---
layout: page
title: Use Jemalloc for CH Backend
nav_order: 12
has_children: true
parent: /developer-overview/
---
We need using jemalloc to find the memory issue. That's what this document is about.

## Change code of jemalloc
Use libunwind instead of libgcc to get the backtrace which may cause process hangs.
```
@@ -177,7 +177,8 @@ target_compile_definitions(_jemalloc PRIVATE -DJEMALLOC_PROF=1)
 # At the time ClickHouse uses LLVM libunwind which follows libgcc's way of backtracking.
 #
 # ClickHouse has to provide `unw_backtrace` method by the means of [commit 8e2b31e](https://github.com/ClickHouse/libunwind/commit/8e2b31e766dd502f6df74909e04a7dbdf5182eb1).
-target_compile_definitions (_jemalloc PRIVATE -DJEMALLOC_PROF_LIBGCC=1)
+#target_compile_definitions (_jemalloc PRIVATE -DJEMALLOC_PROF_LIBGCC=1)
+target_compile_definitions (_jemalloc PRIVATE -DJEMALLOC_PROF_LIBUNWIND=1)
 target_link_libraries (_jemalloc PRIVATE unwind)
 ```
For more infomation, check https://github.com/jemalloc/jemalloc/issues/2282.

## Get jeprof
Change to the directory where you want to install jemalloc, and run the following commands:
```
cd $Clickhouse_SOURCE_PATH/contrib/jemalloc && ./autogen.sh && ./configure.sh && make -j8
```
Then we get jeprof in the directory `$Clickhouse_SOURCE_PATH/contrib/jemalloc/bin/jeprof`.

## Compile libch.so
Ensure to enable jemalloc `-DENABLE_JEMALLOC=ON` in cpp-ch/CMakeLists.txt, and compile libch.so.

## Run Gluten with jemalloc heap tools
For Yarn or thrift server, you need add the following to the submit script:
```
export MALLOC_CONF="prof:true,lg_prof_interval:30" # enable jemalloc heap profiling
```
You can find more options in https://github.com/jemalloc/jemalloc/wiki/Use-Case%3A-Heap-Profiling.

## Analyze the result
After you get the heap file, you can use the following command to analyze the result:
Check memory diff, so you can find which part of the code consume the memory.
```
jeprof --svg /usr/lib/jvm/java-8-openjdk-amd64/bin/java --base=jeprof.3717358.1.i1.heap jeprof.3717358.30.i30.heap > diff.svg
```
You can find more usage about jeprof with `jeprof --help`.
---
layout: page
title: Debug for CH Backend with gpertools Tool
nav_order: 11
has_children: true
parent: /developer-overview/
---
We need using gpertools to find the memory or CPU issue. That's what this document is about.

## Install gperftools
Install gperftools as described in https://github.com/gperftools/gperftools.
We get the library and the command line tools.

## Compile libch.so
Disable jemalloc `-DENABLE_JEMALLOC=OFF` in cpp-ch/CMakeLists.txt, and recompile libch.so.

## Run Gluten with gperftools
For Spark on Yarn, we can change the submit script to run Gluten with gperftools.
Add the following to the submit script:
```
export tcmalloc_path=/data2/zzb/gperftools-2.10/.libs/libtcmalloc_and_profiler.so # the path to the tcmalloc library
export LD_PRELOAD=$tcmalloc_path,libch.so # load the library in the driver
--files $tcmalloc_path # upload the library to the cluster
--conf spark.executorEnv.LD_PRELOAD=./libtcmalloc_and_profiler.so,libch.so # load the library in the executor
--conf spark.executorEnv.HEAPPROFILE=/tmp/gluten_heap_perf # set the heap profile path, you can change to CPUPROFILE for CPU profiling
```

For thrift server on local machine, note using `export LD_PRELOAD="$tcmalloc_path libch.so" # load the library in the driver` to preload dynamic libraries.

## Analyze the result
We can get the result in the path we set in the previous step. For example, we can get the result in `/tmp/gluten_heap_perf`. We can use the following website to analyze the result:
https://gperftools.github.io/gperftools/heapprofile.html
https://gperftools.github.io/gperftools/cpuprofile.html

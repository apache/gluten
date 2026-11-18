---
layout: page
title: Velox Backend Troubleshooting
nav_order: 10
---
## Troubleshooting

### Fatal error after native exception is thrown

We depend on checking exceptions thrown from native code to validate whether a spark plan can
be really offloaded to native engine. But if `libunwind-dev` is installed, native exception will not be
caught and will interrupt the program. So far, we observed this fatal error can happen only on Ubuntu 20.04.
Please remove `libunwind-dev` and then re-build the project to address this issue.

`sudo apt-get purge --auto-remove libunwind-dev`

### Jar conflict issue

With the latest version of Gluten, there should not be any jar conflict issue anymore. If you still get hit with
such issue, please follow the below instructions.

The potentially conflicting libraries include protobuf (Both Velox and CK backend), flatbuffers (Velox backend), and arrow-* (Velox backend).
These libraries are compiled from source and packed into Gluten's jar. Jvm should search them from Gluten.jar firstly and load them. But for
some reason jvm loads the jars from spark_home/jars which causes conflict. You may use below commands to remove the jars from spark_home/jars.
We are still investigating the root cause. Welcome to share if you have good solution.

```
rm -rf $SPARK_HOME/jars/protobuf-*
# velox backend only
rm -rf $SPARK_HOME/jars/flatbuffers-*
rm -rf $SPARK_HOME/jars/arrow-*
```

### Incompatible class error when using native writer
Gluten native writer overwrite some vanilla spark classes. Therefore, when running a program that uses gluten, it is essential to ensure that
the gluten jar is loaded prior to the vanilla spark jar. In this section, we will provide some configuration settings in
`$SPARK_HOME/conf/spark-defaults.conf` for Yarn client, Yarn cluster, and Local&Standalone mode to guarantee that the gluten jar is prioritized.

#### Configurations for Yarn Client mode

```
// spark will upload the gluten jar to hdfs and then the nodemanager will fetch the gluten jar before start the executor process. Here also can set the spark.jars.
spark.files=<absolute_path_to_Gluten_JAR>
// The absolute path on running node
spark.driver.extraClassPath=<absolute_path_to_Gluten_JAR>
// The relative path under the executor working directory
spark.executor.extraClassPath=./<Gluten_JAR>
```

#### Configurations for Yarn Cluster mode
```
spark.driver.userClassPathFirst=true
spark.executor.userClassPathFirst=true

spark.files=<absolute_path_to_Gluten_JAR>
// The relative path under the executor working directory
spark.driver.extraClassPath=./<Gluten_JAR>
// The relative path under the executor working directory
spark.executor.extraClassPath=./<Gluten_JAR>
```
#### Configurations for Local & Standalone mode
```
// The absolute path on running node
spark.driver.extraClassPath=<absolute_path_to_Gluten_JAR>
// The absolute path on running node
spark.executor.extraClassPath=<absolute_path_to_Gluten_JAR>
```

### Invalid pointer error

If the below error is reported at runtime, please re-build gluten with `--compile_arrow_java=ON`, then redeploy Gluten jar.

```
*** Error in `/usr/local/jdk1.8.0_381/bin/java': free(): invalid pointer: 0x00007f36cb5cec80 ***
======= Backtrace: =========
/lib64/libc.so.6(+0x7d1fd)[0x7f38c29da1fd]
/lib64/libstdc++.so.6(_ZNSt6locale5_Impl16_M_install_facetEPKNS_2idEPKNS_5facetE+0x142)[0x7f36cb3370d2]
/lib64/libstdc++.so.6(_ZNSt6locale5_ImplC1Em+0x1e3)[0x7f36cb337523]
/lib64/libstdc++.so.6(+0x71495)[0x7f36cb338495]
/lib64/libpthread.so.0(pthread_once+0x50)[0x7f38c3147be0]
/lib64/libstdc++.so.6(+0x714e1)[0x7f36cb3384e1]
/lib64/libstdc++.so.6(_ZNSt6localeC2Ev+0x13)[0x7f36cb338523]
/lib64/libstdc++.so.6(_ZNSt8ios_base4InitC2Ev+0xbc)[0x7f36cb33537c]
/tmp/jnilib-645156599284574767.tmp(+0x2a90)[0x7f375d235a90]
/lib64/ld-linux-x86-64.so.2(+0xf4e3)[0x7f38c33664e3]
/lib64/ld-linux-x86-64.so.2(+0x13b04)[0x7f38c336ab04]
/lib64/ld-linux-x86-64.so.2(+0xf2f4)[0x7f38c33662f4]
/lib64/ld-linux-x86-64.so.2(+0x1321b)[0x7f38c336a21b]
/lib64/libdl.so.2(+0x102b)[0x7f38c2d1f02b]
/lib64/ld-linux-x86-64.so.2(+0xf2f4)[0x7f38c33662f4]
/lib64/libdl.so.2(+0x162d)[0x7f38c2d1f62d]
/lib64/libdl.so.2(dlopen+0x31)[0x7f38c2d1f0c1]
/usr/local/jdk1.8.0_381/jre/lib/amd64/server/libjvm.so(+0x9292b1)[0x7f38c22732b1]
/usr/local/jdk1.8.0_381/jre/lib/amd64/server/libjvm.so(JVM_LoadLibrary+0xa1)[0x7f38c205e0c1]
/usr/local/jdk1.8.0_381/jre/lib/amd64/libjava.so(Java_java_lang_ClassLoader_00024NativeLibrary_load+0x1ac)
...
```

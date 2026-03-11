layout: page
title: Spark configurations status in Gluten Velox Backend
nav_order: 17

The file lists the if Spark configurations are hornored by Gluten velox backend or not. Table is from Spark4.0 configuration page. The status are:
- ✅ Supported<br>
- ❌ Not Supported<br>
- ⚠️ Partial Support<br>
- 🔄 In Progress<br>
- 🚫 Not applied or transparent to Gluten<br>
- `<blank>`: unknown yet


# Application Properties
<table>
<tr>
  <th>Property Name</th>
  <th>Default</th>
  <th>Since Version</th>
  <th>Gluten Status</th>  
</tr>
<tr>
  <td><code>spark.app.name</code></td>
  <td>(none)</td>
  <td>0.9.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.driver.cores</code></td>
  <td>1</td>
  <td>1.3.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.driver.maxResultSize</code></td>
  <td>1g</td>
  <td>1.2.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.driver.memory</code></td>
  <td>1g</td>
  <td>1.1.1</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.driver.memoryOverhead</code></td>
  <td>driverMemory * <code>spark.driver.memoryOverheadFactor</code>, with minimum of <code>spark.driver.minMemoryOverhead</code></td>
  <td>2.3.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.driver.minMemoryOverhead</code></td>
  <td>384m</td>
  <td>4.0.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.driver.memoryOverheadFactor</code></td>
  <td>0.10</td>
  <td>3.3.0</td>
  <td></td>
</tr>
<tr>
 <td><code>spark.driver.resource.{resourceName}.amount</code></td>
  <td>0</td>
  <td>3.0.0</td>
  <td></td>
</tr>
<tr>
 <td><code>spark.driver.resource.{resourceName}.discoveryScript</code></td>
  <td>None</td>
  <td>3.0.0</td>
  <td></td>
</tr>
<tr>
 <td><code>spark.driver.resource.{resourceName}.vendor</code></td>
  <td>None</td>
  <td>3.0.0</td>
  <td></td>
</tr>
<tr>
 <td><code>spark.resources.discoveryPlugin</code></td>
  <td>org.apache.spark.resource.ResourceDiscoveryScriptPlugin</td>
  <td>3.0.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.executor.memory</code></td>
  <td>1g</td>
  <td>0.7.0</td>
  <td></td>
</tr>
<tr>
 <td><code>spark.executor.pyspark.memory</code></td>
  <td>Not set</td>
  <td>2.4.0</td>
  <td></td>
</tr>
<tr>
 <td><code>spark.executor.memoryOverhead</code></td>
  <td>executorMemory * <code>spark.executor.memoryOverheadFactor</code>, with minimum of <code>spark.executor.minMemoryOverhead</code></td>
  <td>2.3.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.executor.minMemoryOverhead</code></td>
  <td>384m</td>
  <td>4.0.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.executor.memoryOverheadFactor</code></td>
  <td>0.10</td>
  <td>3.3.0</td>
  <td></td>
</tr>
<tr>
 <td><code>spark.executor.resource.{resourceName}.amount</code></td>
  <td>0</td>
  <td>3.0.0</td>
  <td></td>
</tr>
<tr>
 <td><code>spark.executor.resource.{resourceName}.discoveryScript</code></td>
  <td>None</td>
  <td>3.0.0</td>
  <td></td>
</tr>
<tr>
 <td><code>spark.executor.resource.{resourceName}.vendor</code></td>
  <td>None</td>
  <td>3.0.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.extraListeners</code></td>
  <td>(none)</td>
  <td>1.3.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.local.dir</code></td>
  <td>/tmp</td>
  <td>0.5.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.logConf</code></td>
  <td>false</td>
  <td>0.9.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.master</code></td>
  <td>(none)</td>
  <td>0.9.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.submit.deployMode</code></td>
  <td>client</td>
  <td>1.5.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.log.callerContext</code></td>
  <td>(none)</td>
  <td>2.2.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.log.level</code></td>
  <td>(none)</td>
  <td>3.5.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.driver.supervise</code></td>
  <td>false</td>
  <td>1.3.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.driver.timeout</code></td>
  <td>0min</td>
  <td>4.0.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.driver.log.localDir</code></td>
  <td>(none)</td>
  <td>4.0.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.driver.log.dfsDir</code></td>
  <td>(none)</td>
  <td>3.0.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.driver.log.persistToDfs.enabled</code></td>
  <td>false</td>
  <td>3.0.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.driver.log.layout</code></td>
  <td>%d{yy/MM/dd HH:mm:ss.SSS} %t %p %c{1}: %m%n%ex</td>
  <td>3.0.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.driver.log.allowErasureCoding</code></td>
  <td>false</td>
  <td>3.0.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.decommission.enabled</code></td>
  <td>false</td>
  <td>3.1.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.executor.decommission.killInterval</code></td>
  <td>(none)</td>
  <td>3.1.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.executor.decommission.forceKillTimeout</code></td>
  <td>(none)</td>
  <td>3.2.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.executor.decommission.signal</code></td>
  <td>PWR</td>
  <td>3.2.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.executor.maxNumFailures</code></td>
  <td>numExecutors * 2, with minimum of 3</td>
  <td>3.5.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.executor.failuresValidityInterval</code></td>
  <td>(none)</td>
  <td>3.5.0</td>
  <td></td>
</tr>
</table>

# Runtime Environment

<table class="spark-config">
<thead><tr><th>Property Name</th><th>Default</th><th>Since Version</th><th>Gluten Status</th></tr></thead>
<tr>
  <td><code>spark.driver.extraClassPath</code></td>
  <td>(none)</td>
  <td>1.0.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.driver.defaultJavaOptions</code></td>
  <td>(none)</td>
  <td>3.0.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.driver.extraJavaOptions</code></td>
  <td>(none)</td>
  <td>1.0.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.driver.extraLibraryPath</code></td>
  <td>(none)</td>
  <td>1.0.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.driver.userClassPathFirst</code></td>
  <td>false</td>
  <td>1.3.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.executor.extraClassPath</code></td>
  <td>(none)</td>
  <td>1.0.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.executor.defaultJavaOptions</code></td>
  <td>(none)</td>
  <td>3.0.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.executor.extraJavaOptions</code></td>
  <td>(none)</td>
  <td>1.0.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.executor.extraLibraryPath</code></td>
  <td>(none)</td>
  <td>1.0.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.executor.logs.rolling.maxRetainedFiles</code></td>
  <td>-1</td>
  <td>1.1.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.executor.logs.rolling.enableCompression</code></td>
  <td>false</td>
  <td>2.0.2</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.executor.logs.rolling.maxSize</code></td>
  <td>1024 * 1024</td>
  <td>1.4.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.executor.logs.rolling.strategy</code></td>
  <td>"" (disabled)</td>
  <td>1.1.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.executor.logs.rolling.time.interval</code></td>
  <td>daily</td>
  <td>1.1.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.executor.userClassPathFirst</code></td>
  <td>false</td>
  <td>1.3.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.executorEnv.[EnvironmentVariableName]</code></td>
  <td>(none)</td>
  <td>0.9.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.redaction.regex</code></td>
  <td>(?i)secret|password|token|access[.]?key</td>
  <td>2.1.2</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.redaction.string.regex</code></td>
  <td>(none)</td>
  <td>2.2.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.python.profile</code></td>
  <td>false</td>
  <td>1.2.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.python.profile.dump</code></td>
  <td>(none)</td>
  <td>1.2.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.python.worker.memory</code></td>
  <td>512m</td>
  <td>1.1.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.python.worker.reuse</code></td>
  <td>true</td>
  <td>1.2.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.files</code></td>
  <td></td>
  <td>1.0.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.submit.pyFiles</code></td>
  <td></td>
  <td>1.0.1</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.jars</code></td>
  <td></td>
  <td>0.9.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.jars.packages</code></td>
  <td></td>
  <td>1.5.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.jars.excludes</code></td>
  <td></td>
  <td>1.5.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.jars.ivy</code></td>
  <td></td>
  <td>1.3.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.jars.ivySettings</code></td>
  <td></td>
  <td>2.2.0</td>
  <td></td>
</tr>
 <tr>
  <td><code>spark.jars.repositories</code></td>
  <td></td>
  <td>2.3.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.archives</code></td>
  <td></td>
  <td>3.1.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.pyspark.driver.python</code></td>
  <td></td>
  <td>2.1.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.pyspark.python</code></td>
  <td></td>
  <td>2.1.0</td>
  <td></td>
</tr>
</table>

# Shuffle Behavior

<table class="spark-config">
<thead><tr><th>Property Name</th><th>Default</th><th>Since Version</th><th>Gluten Status</th></tr></thead>
<tr>
  <td><code>spark.reducer.maxSizeInFlight</code></td>
  <td>48m</td>
  <td>1.4.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.reducer.maxReqsInFlight</code></td>
  <td>Int.MaxValue</td>
  <td>2.0.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.reducer.maxBlocksInFlightPerAddress</code></td>
  <td>Int.MaxValue</td>
  <td>2.2.1</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.shuffle.compress</code></td>
  <td>true</td>
  <td>0.6.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.shuffle.file.buffer</code></td>
  <td>32k</td>
  <td>1.4.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.shuffle.file.merge.buffer</code></td>
  <td>32k</td>
  <td>4.0.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.shuffle.unsafe.file.output.buffer</code></td>
  <td>32k</td>
  <td>2.3.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.shuffle.localDisk.file.output.buffer</code></td>
  <td>32k</td>
  <td>4.0.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.shuffle.spill.diskWriteBufferSize</code></td>
  <td>1024 * 1024</td>
  <td>2.3.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.shuffle.io.maxRetries</code></td>
  <td>3</td>
  <td>1.2.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.shuffle.io.numConnectionsPerPeer</code></td>
  <td>1</td>
  <td>1.2.1</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.shuffle.io.preferDirectBufs</code></td>
  <td>true</td>
  <td>1.2.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.shuffle.io.retryWait</code></td>
  <td>5s</td>
  <td>1.2.1</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.shuffle.io.backLog</code></td>
  <td>-1</td>
  <td>1.1.1</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.shuffle.io.connectionTimeout</code></td>
  <td>value of <code>spark.network.timeout</code></td>
  <td>1.2.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.shuffle.io.connectionCreationTimeout</code></td>
  <td>value of <code>spark.shuffle.io.connectionTimeout</code></td>
  <td>3.2.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.shuffle.service.enabled</code></td>
  <td>false</td>
  <td>1.2.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.shuffle.service.port</code></td>
  <td>7337</td>
  <td>1.2.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.shuffle.service.name</code></td>
  <td>spark_shuffle</td>
  <td>3.2.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.shuffle.service.index.cache.size</code></td>
  <td>100m</td>
  <td>2.3.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.shuffle.service.removeShuffle</code></td>
  <td>true</td>
  <td>3.3.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.shuffle.maxChunksBeingTransferred</code></td>
  <td>Long.MAX_VALUE</td>
  <td>2.3.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.shuffle.sort.bypassMergeThreshold</code></td>
  <td>200</td>
  <td>1.1.1</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.shuffle.sort.io.plugin.class</code></td>
  <td>org.apache.spark.shuffle.sort.io.LocalDiskShuffleDataIO</td>
  <td>3.0.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.shuffle.spill.compress</code></td>
  <td>true</td>
  <td>0.9.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.shuffle.accurateBlockThreshold</code></td>
  <td>100 * 1024 * 1024</td>
  <td>2.2.1</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.shuffle.accurateBlockSkewedFactor</code></td>
  <td>-1.0</td>
  <td>3.3.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.shuffle.registration.timeout</code></td>
  <td>5000</td>
  <td>2.3.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.shuffle.registration.maxAttempts</code></td>
  <td>3</td>
  <td>2.3.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.shuffle.reduceLocality.enabled</code></td>
  <td>true</td>
  <td>1.5.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.shuffle.mapOutput.minSizeForBroadcast</code></td>
  <td>512k</td>
  <td>2.0.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.shuffle.detectCorrupt</code></td>
  <td>true</td>
  <td>2.2.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.shuffle.detectCorrupt.useExtraMemory</code></td>
  <td>false</td>
  <td>3.0.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.shuffle.useOldFetchProtocol</code></td>
  <td>false</td>
  <td>3.0.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.shuffle.readHostLocalDisk</code></td>
  <td>true</td>
  <td>3.0.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.files.io.connectionTimeout</code></td>
  <td>value of <code>spark.network.timeout</code></td>
  <td>1.6.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.files.io.connectionCreationTimeout</code></td>
  <td>value of <code>spark.files.io.connectionTimeout</code></td>
  <td>3.2.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.shuffle.checksum.enabled</code></td>
  <td>true</td>
  <td>3.2.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.shuffle.checksum.algorithm</code></td>
  <td>ADLER32</td>
  <td>3.2.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.shuffle.service.fetch.rdd.enabled</code></td>
  <td>false</td>
  <td>3.0.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.shuffle.service.db.enabled</code></td>
  <td>true</td>
  <td>3.0.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.shuffle.service.db.backend</code></td>
  <td>ROCKSDB</td>
  <td>3.4.0</td>
  <td></td>
</tr>
</table>

# Spark UI

<table class="spark-config">
<thead><tr><th>Property Name</th><th>Default</th><th>Since Version</th><th>Gluten Status</th></tr></thead>
<tr>
  <td><code>spark.eventLog.logBlockUpdates.enabled</code></td>
  <td>false</td>
  <td>2.3.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.eventLog.longForm.enabled</code></td>
  <td>false</td>
  <td>2.4.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.eventLog.compress</code></td>
  <td>true</td>
  <td>1.0.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.eventLog.compression.codec</code></td>
  <td>zstd</td>
  <td>3.0.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.eventLog.erasureCoding.enabled</code></td>
  <td>false</td>
  <td>3.0.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.eventLog.dir</code></td>
  <td>file:///tmp/spark-events</td>
  <td>1.0.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.eventLog.enabled</code></td>
  <td>false</td>
  <td>1.0.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.eventLog.overwrite</code></td>
  <td>false</td>
  <td>1.0.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.eventLog.buffer.kb</code></td>
  <td>100k</td>
  <td>1.0.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.eventLog.rolling.enabled</code></td>
  <td>false</td>
  <td>3.0.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.eventLog.rolling.maxFileSize</code></td>
  <td>128m</td>
  <td>3.0.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.ui.dagGraph.retainedRootRDDs</code></td>
  <td>Int.MaxValue</td>
  <td>2.1.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.ui.groupSQLSubExecutionEnabled</code></td>
  <td>true</td>
  <td>3.4.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.ui.enabled</code></td>
  <td>true</td>
  <td>1.1.1</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.ui.store.path</code></td>
  <td>None</td>
  <td>3.4.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.ui.killEnabled</code></td>
  <td>true</td>
  <td>1.0.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.ui.threadDumpsEnabled</code></td>
  <td>true</td>
  <td>1.2.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.ui.threadDump.flamegraphEnabled</code></td>
  <td>true</td>
  <td>4.0.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.ui.heapHistogramEnabled</code></td>
  <td>true</td>
  <td>3.5.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.ui.liveUpdate.period</code></td>
  <td>100ms</td>
  <td>2.3.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.ui.liveUpdate.minFlushPeriod</code></td>
  <td>1s</td>
  <td>2.4.2</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.ui.port</code></td>
  <td>4040</td>
  <td>0.7.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.ui.retainedJobs</code></td>
  <td>1000</td>
  <td>1.2.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.ui.retainedStages</code></td>
  <td>1000</td>
  <td>0.9.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.ui.retainedTasks</code></td>
  <td>100000</td>
  <td>2.0.1</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.ui.reverseProxy</code></td>
  <td>false</td>
  <td>2.1.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.ui.reverseProxyUrl</code></td>
  <td></td>
  <td>2.1.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.ui.proxyRedirectUri</code></td>
  <td></td>
  <td>3.0.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.ui.showConsoleProgress</code></td>
  <td>false</td>
  <td>1.2.1</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.ui.consoleProgress.update.interval</code></td>
  <td>200</td>
  <td>2.1.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.ui.custom.executor.log.url</code></td>
  <td>(none)</td>
  <td>3.0.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.ui.prometheus.enabled</code></td>
  <td>true</td>
  <td>3.0.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.worker.ui.retainedExecutors</code></td>
  <td>1000</td>
  <td>1.5.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.worker.ui.retainedDrivers</code></td>
  <td>1000</td>
  <td>1.5.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.sql.ui.retainedExecutions</code></td>
  <td>1000</td>
  <td>1.5.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.streaming.ui.retainedBatches</code></td>
  <td>1000</td>
  <td>1.0.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.ui.retainedDeadExecutors</code></td>
  <td>100</td>
  <td>2.0.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.ui.filters</code></td>
  <td>None</td>
  <td>1.0.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.ui.requestHeaderSize</code></td>
  <td>8k</td>
  <td>2.2.3</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.ui.timelineEnabled</code></td>
  <td>true</td>
  <td>3.4.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.ui.timeline.executors.maximum</code></td>
  <td>250</td>
  <td>3.2.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.ui.timeline.jobs.maximum</code></td>
  <td>500</td>
  <td>3.2.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.ui.timeline.stages.maximum</code></td>
  <td>500</td>
  <td>3.2.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.ui.timeline.tasks.maximum</code></td>
  <td>1000</td>
  <td>1.4.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.appStatusStore.diskStoreDir</code></td>
  <td>None</td>
  <td>3.4.0</td>
  <td></td>
</tr>
</table>

# Compression and Serialization

<table class="spark-config">
<thead><tr><th>Property Name</th><th>Default</th><th>Since Version</th><th>Gluten Status</th></tr></thead>
<tr>
  <td><code>spark.broadcast.compress</code></td>
  <td>true</td>
  <td>0.6.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.checkpoint.dir</code></td>
  <td>(none)</td>
  <td>4.0.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.checkpoint.compress</code></td>
  <td>false</td>
  <td>2.2.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.io.compression.codec</code></td>
  <td>lz4</td>
  <td>0.8.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.io.compression.lz4.blockSize</code></td>
  <td>32k</td>
  <td>1.4.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.io.compression.snappy.blockSize</code></td>
  <td>32k</td>
  <td>1.4.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.io.compression.zstd.level</code></td>
  <td>1</td>
  <td>2.3.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.io.compression.zstd.bufferSize</code></td>
  <td>32k</td>
  <td>2.3.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.io.compression.zstd.bufferPool.enabled</code></td>
  <td>true</td>
  <td>3.2.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.io.compression.zstd.workers</code></td>
  <td>0</td>
  <td>4.0.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.io.compression.lzf.parallel.enabled</code></td>
  <td>false</td>
  <td>4.0.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.kryo.classesToRegister</code></td>
  <td>(none)</td>
  <td>1.2.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.kryo.referenceTracking</code></td>
  <td>true</td>
  <td>0.8.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.kryo.registrationRequired</code></td>
  <td>false</td>
  <td>1.1.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.kryo.registrator</code></td>
  <td>(none)</td>
  <td>0.5.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.kryo.unsafe</code></td>
  <td>true</td>
  <td>2.1.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.kryoserializer.buffer.max</code></td>
  <td>64m</td>
  <td>1.4.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.kryoserializer.buffer</code></td>
  <td>64k</td>
  <td>1.4.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.rdd.compress</code></td>
  <td>false</td>
  <td>0.6.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.serializer</code></td>
  <td>
    org.apache.spark.serializer.<br />JavaSerializer
  </td>
  <td>0.5.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.serializer.objectStreamReset</code></td>
  <td>100</td>
  <td>1.0.0</td>
  <td></td>
</tr>
</table>

# Memory Management

<table class="spark-config">
<thead><tr><th>Property Name</th><th>Default</th><th>Since Version</th><th>Gluten Status</th></tr></thead>
<tr>
  <td><code>spark.memory.fraction</code></td>
  <td>0.6</td>
  <td>1.6.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.memory.storageFraction</code></td>
  <td>0.5</td>
  <td>1.6.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.memory.offHeap.enabled</code></td>
  <td>false</td>
  <td>1.6.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.memory.offHeap.size</code></td>
  <td>0</td>
  <td>1.6.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.storage.unrollMemoryThreshold</code></td>
  <td>1024 * 1024</td>
  <td>1.1.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.storage.replication.proactive</code></td>
  <td>true</td>
  <td>2.2.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.storage.localDiskByExecutors.cacheSize</code></td>
  <td>1000</td>
  <td>3.0.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.cleaner.periodicGC.interval</code></td>
  <td>30min</td>
  <td>1.6.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.cleaner.referenceTracking</code></td>
  <td>true</td>
  <td>1.0.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.cleaner.referenceTracking.blocking</code></td>
  <td>true</td>
  <td>1.0.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.cleaner.referenceTracking.blocking.shuffle</code></td>
  <td>false</td>
  <td>1.1.1</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.cleaner.referenceTracking.cleanCheckpoints</code></td>
  <td>false</td>
  <td>1.4.0</td>
  <td></td>
</tr>
</table>

# Execution Behavior

<table class="spark-config">
<thead><tr><th>Property Name</th><th>Default</th><th>Since Version</th><th>Gluten Status</th></tr></thead>
<tr>
  <td><code>spark.broadcast.blockSize</code></td>
  <td>4m</td>
  <td>0.5.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.broadcast.checksum</code></td>
  <td>true</td>
  <td>2.1.1</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.broadcast.UDFCompressionThreshold</code></td>
  <td>1 * 1024 * 1024</td>
  <td>3.0.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.executor.cores</code></td>
  <td>
    1 in YARN mode, all the available cores on the worker in standalone mode.
  </td>
  <td>1.0.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.default.parallelism</code></td>
  <td>
    For distributed shuffle operations like <code>reduceByKey</code> and <code>join</code>, the
    largest number of partitions in a parent RDD.  For operations like <code>parallelize</code>
    with no parent RDDs, it depends on the cluster manager:
    <ul>
      <li>Local mode: number of cores on the local machine</li>
      <li>Others: total number of cores on all executor nodes or 2, whichever is larger</li>
    </ul>
  </td>
  <td>0.5.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.executor.heartbeatInterval</code></td>
  <td>10s</td>
  <td>1.1.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.files.fetchTimeout</code></td>
  <td>60s</td>
  <td>1.0.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.files.useFetchCache</code></td>
  <td>true</td>
  <td>1.2.2</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.files.overwrite</code></td>
  <td>false</td>
  <td>1.0.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.files.ignoreCorruptFiles</code></td>
  <td>false</td>
  <td>2.1.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.files.ignoreMissingFiles</code></td>
  <td>false</td>
  <td>2.4.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.files.maxPartitionBytes</code></td>
  <td>134217728 (128 MiB)</td>
  <td>2.1.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.files.openCostInBytes</code></td>
  <td>4194304 (4 MiB)</td>
  <td>2.1.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.hadoop.cloneConf</code></td>
  <td>false</td>
  <td>1.0.3</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.hadoop.validateOutputSpecs</code></td>
  <td>true</td>
  <td>1.0.1</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.storage.memoryMapThreshold</code></td>
  <td>2m</td>
  <td>0.9.2</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.storage.decommission.enabled</code></td>
  <td>false</td>
  <td>3.1.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.storage.decommission.shuffleBlocks.enabled</code></td>
  <td>true</td>
  <td>3.1.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.storage.decommission.shuffleBlocks.maxThreads</code></td>
  <td>8</td>
  <td>3.1.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.storage.decommission.rddBlocks.enabled</code></td>
  <td>true</td>
  <td>3.1.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.storage.decommission.fallbackStorage.path</code></td>
  <td>(none)</td>
  <td>3.1.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.storage.decommission.fallbackStorage.cleanUp</code></td>
  <td>false</td>
  <td>3.2.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.storage.decommission.shuffleBlocks.maxDiskSize</code></td>
  <td>(none)</td>
  <td>3.2.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version</code></td>
  <td>1</td>
  <td>2.2.0</td>
  <td></td>
</tr>
</table>

# Executor Metrics
These configurations are handled by Spark and do not affect Gluten’s behavior.

# Networking
These configurations are handled by Spark and do not affect Gluten’s behavior.

# Scheduling
<table class="spark-config">
<thead><tr><th>Property Name</th><th>Default</th><th>Since Version</th><th></th></tr></thead>
<tr>
  <td><code>spark.cores.max</code></td>
  <td>(not set)</td>
  <td>0.6.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.locality.wait</code></td>
  <td>3s</td>
  <td>0.5.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.locality.wait.node</code></td>
  <td>spark.locality.wait</td>
  <td>0.8.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.locality.wait.process</code></td>
  <td>spark.locality.wait</td>
  <td>0.8.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.locality.wait.rack</code></td>
  <td>spark.locality.wait</td>
  <td>0.8.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.scheduler.maxRegisteredResourcesWaitingTime</code></td>
  <td>30s</td>
  <td>1.1.1</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.scheduler.minRegisteredResourcesRatio</code></td>
  <td>0.8 for KUBERNETES mode; 0.8 for YARN mode; 0.0 for standalone mode</td>
  <td>1.1.1</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.scheduler.mode</code></td>
  <td>FIFO</td>
  <td>0.8.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.scheduler.revive.interval</code></td>
  <td>1s</td>
  <td>0.8.1</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.scheduler.listenerbus.eventqueue.capacity</code></td>
  <td>10000</td>
  <td>2.3.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.scheduler.listenerbus.eventqueue.shared.capacity</code></td>
  <td><code>spark.scheduler.listenerbus.eventqueue.capacity</code></td>
  <td>3.0.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.scheduler.listenerbus.eventqueue.appStatus.capacity</code></td>
  <td><code>spark.scheduler.listenerbus.eventqueue.capacity</code></td>
  <td>3.0.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.scheduler.listenerbus.eventqueue.executorManagement.capacity</code></td>
  <td><code>spark.scheduler.listenerbus.eventqueue.capacity</code></td>
  <td>3.0.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.scheduler.listenerbus.eventqueue.eventLog.capacity</code></td>
  <td><code>spark.scheduler.listenerbus.eventqueue.capacity</code></td>
  <td>3.0.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.scheduler.listenerbus.eventqueue.streams.capacity</code></td>
  <td><code>spark.scheduler.listenerbus.eventqueue.capacity</code></td>
  <td>3.0.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.scheduler.resource.profileMergeConflicts</code></td>
  <td>false</td>
  <td>3.1.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.scheduler.excludeOnFailure.unschedulableTaskSetTimeout</code></td>
  <td>120s</td>
  <td>2.4.1</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.standalone.submit.waitAppCompletion</code></td>
  <td>false</td>
  <td>3.1.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.excludeOnFailure.enabled</code></td>
  <td>
    false
  </td>
  <td>2.1.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.excludeOnFailure.application.enabled</code></td>
  <td>
    false
  </td>
  <td>4.0.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.excludeOnFailure.taskAndStage.enabled</code></td>
  <td>
    false
  </td>
  <td>4.0.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.excludeOnFailure.timeout</code></td>
  <td>1h</td>
  <td>2.1.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.excludeOnFailure.task.maxTaskAttemptsPerExecutor</code></td>
  <td>1</td>
  <td>2.1.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.excludeOnFailure.task.maxTaskAttemptsPerNode</code></td>
  <td>2</td>
  <td>2.1.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.excludeOnFailure.stage.maxFailedTasksPerExecutor</code></td>
  <td>2</td>
  <td>2.1.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.excludeOnFailure.stage.maxFailedExecutorsPerNode</code></td>
  <td>2</td>
  <td>2.1.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.excludeOnFailure.application.maxFailedTasksPerExecutor</code></td>
  <td>2</td>
  <td>2.2.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.excludeOnFailure.application.maxFailedExecutorsPerNode</code></td>
  <td>2</td>
  <td>2.2.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.excludeOnFailure.killExcludedExecutors</code></td>
  <td>false</td>
  <td>2.2.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.excludeOnFailure.application.fetchFailure.enabled</code></td>
  <td>false</td>
  <td>2.3.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.speculation</code></td>
  <td>false</td>
  <td>0.6.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.speculation.interval</code></td>
  <td>100ms</td>
  <td>0.6.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.speculation.multiplier</code></td>
  <td>3</td>
  <td>0.6.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.speculation.quantile</code></td>
  <td>0.9</td>
  <td>0.6.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.speculation.minTaskRuntime</code></td>
  <td>100ms</td>
  <td>3.2.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.speculation.task.duration.threshold</code></td>
  <td>None</td>
  <td>3.0.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.speculation.efficiency.processRateMultiplier</code></td>
  <td>0.75</td>
  <td>3.4.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.speculation.efficiency.longRunTaskFactor</code></td>
  <td>2</td>
  <td>3.4.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.speculation.efficiency.enabled</code></td>
  <td>true</td>
  <td>3.4.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.task.cpus</code></td>
  <td>1</td>
  <td>0.5.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.task.resource.{resourceName}.amount</code></td>
  <td>1</td>
  <td>3.0.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.task.maxFailures</code></td>
  <td>4</td>
  <td>0.8.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.task.reaper.enabled</code></td>
  <td>false</td>
  <td>2.0.3</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.task.reaper.pollingInterval</code></td>
  <td>10s</td>
  <td>2.0.3</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.task.reaper.threadDump</code></td>
  <td>true</td>
  <td>2.0.3</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.task.reaper.killTimeout</code></td>
  <td>-1</td>
  <td>2.0.3</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.stage.maxConsecutiveAttempts</code></td>
  <td>4</td>
  <td>2.2.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.stage.ignoreDecommissionFetchFailure</code></td>
  <td>true</td>
  <td>3.4.0</td>
  <td></td>
</tr>
</table>

# Barrier Execution Mode
These configurations are handled by Spark and do not affect Gluten’s behavior.

# Dynamic Allocation
These configurations are handled by Spark and do not affect Gluten’s behavior.

# Thread Configurations
These configurations are handled by Spark and do not affect Gluten’s behavior.

# Spark Connect
## Server Configuration
These configurations are handled by Spark and do not affect Gluten’s behavior.

# Security
These configurations are handled by Spark and do not affect Gluten’s behavior.

# Spark SQL

## Runtime SQL Configuration
<table class="spark-config">
<thead><tr><th>Property Name</th><th>Default</th><th>Since Version</th><th>Gluten Status</th></tr></thead>

<tr>
    <td><code>spark.sql.adaptive.advisoryPartitionSizeInBytes</code></td>
    <td>(value of <code>spark.sql.adaptive.shuffle.targetPostShuffleInputSize</code>)</td>
    <td>3.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.adaptive.autoBroadcastJoinThreshold</code></td>
    <td>(none)</td>
    <td>3.2.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.adaptive.coalescePartitions.enabled</code></td>
    <td>true</td>
    <td>3.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.adaptive.coalescePartitions.initialPartitionNum</code></td>
    <td>(none)</td>
    <td>3.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.adaptive.coalescePartitions.minPartitionSize</code></td>
    <td>1MB</td>
    <td>3.2.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.adaptive.coalescePartitions.parallelismFirst</code></td>
    <td>true</td>
    <td>3.2.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.adaptive.customCostEvaluatorClass</code></td>
    <td>(none)</td>
    <td>3.2.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.adaptive.enabled</code></td>
    <td>true</td>
    <td>1.6.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.adaptive.forceOptimizeSkewedJoin</code></td>
    <td>false</td>
    <td>3.3.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.adaptive.localShuffleReader.enabled</code></td>
    <td>true</td>
    <td>3.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold</code></td>
    <td>0b</td>
    <td>3.2.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.adaptive.optimizeSkewsInRebalancePartitions.enabled</code></td>
    <td>true</td>
    <td>3.2.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.adaptive.optimizer.excludedRules</code></td>
    <td>(none)</td>
    <td>3.1.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.adaptive.rebalancePartitionsSmallPartitionFactor</code></td>
    <td>0.2</td>
    <td>3.3.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.adaptive.skewJoin.enabled</code></td>
    <td>true</td>
    <td>3.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.adaptive.skewJoin.skewedPartitionFactor</code></td>
    <td>5.0</td>
    <td>3.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes</code></td>
    <td>256MB</td>
    <td>3.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.allowNamedFunctionArguments</code></td>
    <td>true</td>
    <td>3.5.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.ansi.doubleQuotedIdentifiers</code></td>
    <td>false</td>
    <td>3.4.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.ansi.enabled</code></td>
    <td>true</td>
    <td>3.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.ansi.enforceReservedKeywords</code></td>
    <td>false</td>
    <td>3.3.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.ansi.relationPrecedence</code></td>
    <td>false</td>
    <td>3.4.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.autoBroadcastJoinThreshold</code></td>
    <td>10MB</td>
    <td>1.1.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.avro.compression.codec</code></td>
    <td>snappy</td>
    <td>2.4.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.avro.deflate.level</code></td>
    <td>-1</td>
    <td>2.4.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.avro.filterPushdown.enabled</code></td>
    <td>true</td>
    <td>3.1.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.avro.xz.level</code></td>
    <td>6</td>
    <td>4.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.avro.zstandard.bufferPool.enabled</code></td>
    <td>false</td>
    <td>4.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.avro.zstandard.level</code></td>
    <td>3</td>
    <td>4.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.binaryOutputStyle</code></td>
    <td>(none)</td>
    <td>4.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.broadcastTimeout</code></td>
    <td>300</td>
    <td>1.3.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.bucketing.coalesceBucketsInJoin.enabled</code></td>
    <td>false</td>
    <td>3.1.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.bucketing.coalesceBucketsInJoin.maxBucketRatio</code></td>
    <td>4</td>
    <td>3.1.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.catalog.spark_catalog</code></td>
    <td>builtin</td>
    <td>3.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.cbo.enabled</code></td>
    <td>false</td>
    <td>2.2.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.cbo.joinReorder.dp.star.filter</code></td>
    <td>false</td>
    <td>2.2.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.cbo.joinReorder.dp.threshold</code></td>
    <td>12</td>
    <td>2.2.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.cbo.joinReorder.enabled</code></td>
    <td>false</td>
    <td>2.2.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.cbo.planStats.enabled</code></td>
    <td>false</td>
    <td>3.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.cbo.starSchemaDetection</code></td>
    <td>false</td>
    <td>2.2.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.charAsVarchar</code></td>
    <td>false</td>
    <td>3.3.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.chunkBase64String.enabled</code></td>
    <td>true</td>
    <td>3.5.2</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.cli.print.header</code></td>
    <td>false</td>
    <td>3.2.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.columnNameOfCorruptRecord</code></td>
    <td>_corrupt_record</td>
    <td>1.2.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.csv.filterPushdown.enabled</code></td>
    <td>true</td>
    <td>3.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.datetime.java8API.enabled</code></td>
    <td>false</td>
    <td>3.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.debug.maxToStringFields</code></td>
    <td>25</td>
    <td>3.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.defaultCacheStorageLevel</code></td>
    <td>MEMORY_AND_DISK</td>
    <td>4.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.defaultCatalog</code></td>
    <td>spark_catalog</td>
    <td>3.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.error.messageFormat</code></td>
    <td>PRETTY</td>
    <td>3.4.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.execution.arrow.enabled</code></td>
    <td>false</td>
    <td>2.3.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.execution.arrow.fallback.enabled</code></td>
    <td>true</td>
    <td>2.4.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.execution.arrow.localRelationThreshold</code></td>
    <td>48MB</td>
    <td>3.4.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.execution.arrow.maxRecordsPerBatch</code></td>
    <td>10000</td>
    <td>2.3.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.execution.arrow.pyspark.enabled</code></td>
    <td>(value of <code>spark.sql.execution.arrow.enabled</code>)</td>
    <td>3.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.execution.arrow.pyspark.fallback.enabled</code></td>
    <td>(value of <code>spark.sql.execution.arrow.fallback.enabled</code>)</td>
    <td>3.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.execution.arrow.pyspark.selfDestruct.enabled</code></td>
    <td>false</td>
    <td>3.2.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.execution.arrow.sparkr.enabled</code></td>
    <td>false</td>
    <td>3.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.execution.arrow.transformWithStateInPandas.maxRecordsPerBatch</code></td>
    <td>10000</td>
    <td>4.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.execution.arrow.useLargeVarTypes</code></td>
    <td>false</td>
    <td>3.5.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.execution.interruptOnCancel</code></td>
    <td>true</td>
    <td>4.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.execution.pandas.inferPandasDictAsMap</code></td>
    <td>false</td>
    <td>4.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.execution.pandas.structHandlingMode</code></td>
    <td>legacy</td>
    <td>3.5.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.execution.pandas.udf.buffer.size</code></td>
    <td>(value of <code>spark.buffer.size</code>)</td>
    <td>3.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.execution.pyspark.udf.faulthandler.enabled</code></td>
    <td>(value of <code>spark.python.worker.faulthandler.enabled</code>)</td>
    <td>4.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.execution.pyspark.udf.hideTraceback.enabled</code></td>
    <td>false</td>
    <td>4.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.execution.pyspark.udf.idleTimeoutSeconds</code></td>
    <td>(value of <code>spark.python.worker.idleTimeoutSeconds</code>)</td>
    <td>4.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.execution.pyspark.udf.simplifiedTraceback.enabled</code></td>
    <td>true</td>
    <td>3.1.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.execution.python.udf.buffer.size</code></td>
    <td>(value of <code>spark.buffer.size</code>)</td>
    <td>4.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.execution.python.udf.maxRecordsPerBatch</code></td>
    <td>100</td>
    <td>4.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.execution.pythonUDF.arrow.concurrency.level</code></td>
    <td>(none)</td>
    <td>4.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.execution.pythonUDF.arrow.enabled</code></td>
    <td>false</td>
    <td>3.4.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.execution.pythonUDTF.arrow.enabled</code></td>
    <td>false</td>
    <td>3.5.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.execution.topKSortFallbackThreshold</code></td>
    <td>2147483632</td>
    <td>2.4.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.extendedExplainProviders</code></td>
    <td>(none)</td>
    <td>4.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.files.ignoreCorruptFiles</code></td>
    <td>false</td>
    <td>2.1.1</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.files.ignoreInvalidPartitionPaths</code></td>
    <td>false</td>
    <td>4.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.files.ignoreMissingFiles</code></td>
    <td>false</td>
    <td>2.3.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.files.maxPartitionBytes</code></td>
    <td>128MB</td>
    <td>2.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.files.maxPartitionNum</code></td>
    <td>(none)</td>
    <td>3.5.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.files.maxRecordsPerFile</code></td>
    <td>0</td>
    <td>2.2.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.files.minPartitionNum</code></td>
    <td>(none)</td>
    <td>3.1.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.function.concatBinaryAsString</code></td>
    <td>false</td>
    <td>2.3.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.function.eltOutputAsString</code></td>
    <td>false</td>
    <td>2.3.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.groupByAliases</code></td>
    <td>true</td>
    <td>2.2.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.groupByOrdinal</code></td>
    <td>true</td>
    <td>2.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.hive.convertInsertingPartitionedTable</code></td>
    <td>true</td>
    <td>3.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.hive.convertInsertingUnpartitionedTable</code></td>
    <td>true</td>
    <td>4.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.hive.convertMetastoreCtas</code></td>
    <td>true</td>
    <td>3.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.hive.convertMetastoreInsertDir</code></td>
    <td>true</td>
    <td>3.3.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.hive.convertMetastoreOrc</code></td>
    <td>true</td>
    <td>2.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.hive.convertMetastoreParquet</code></td>
    <td>true</td>
    <td>1.1.1</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.hive.convertMetastoreParquet.mergeSchema</code></td>
    <td>false</td>
    <td>1.3.1</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.hive.dropPartitionByName.enabled</code></td>
    <td>false</td>
    <td>3.4.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.hive.filesourcePartitionFileCacheSize</code></td>
    <td>262144000</td>
    <td>2.1.1</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.hive.manageFilesourcePartitions</code></td>
    <td>true</td>
    <td>2.1.1</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.hive.metastorePartitionPruning</code></td>
    <td>true</td>
    <td>1.5.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.hive.metastorePartitionPruningFallbackOnException</code></td>
    <td>false</td>
    <td>3.3.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.hive.metastorePartitionPruningFastFallback</code></td>
    <td>false</td>
    <td>3.3.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.hive.thriftServer.async</code></td>
    <td>true</td>
    <td>1.5.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.icu.caseMappings.enabled</code></td>
    <td>true</td>
    <td>4.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.inMemoryColumnarStorage.batchSize</code></td>
    <td>10000</td>
    <td>1.1.1</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.inMemoryColumnarStorage.compressed</code></td>
    <td>true</td>
    <td>1.0.1</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.inMemoryColumnarStorage.enableVectorizedReader</code></td>
    <td>true</td>
    <td>2.3.1</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.inMemoryColumnarStorage.hugeVectorReserveRatio</code></td>
    <td>1.2</td>
    <td>4.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.inMemoryColumnarStorage.hugeVectorThreshold</code></td>
    <td>-1b</td>
    <td>4.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.json.filterPushdown.enabled</code></td>
    <td>true</td>
    <td>3.1.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.json.useUnsafeRow</code></td>
    <td>false</td>
    <td>4.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.jsonGenerator.ignoreNullFields</code></td>
    <td>true</td>
    <td>3.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.leafNodeDefaultParallelism</code></td>
    <td>(none)</td>
    <td>3.2.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.mapKeyDedupPolicy</code></td>
    <td>EXCEPTION</td>
    <td>3.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.maven.additionalRemoteRepositories</code></td>
    <td>https://maven-central.storage-download.googleapis.com/maven2/</td>
    <td>3.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.maxMetadataStringLength</code></td>
    <td>100</td>
    <td>3.1.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.maxPlanStringLength</code></td>
    <td>2147483632</td>
    <td>3.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.maxSinglePartitionBytes</code></td>
    <td>128m</td>
    <td>3.4.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.operatorPipeSyntaxEnabled</code></td>
    <td>true</td>
    <td>4.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.optimizer.avoidCollapseUDFWithExpensiveExpr</code></td>
    <td>true</td>
    <td>4.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.optimizer.collapseProjectAlwaysInline</code></td>
    <td>false</td>
    <td>3.3.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.optimizer.dynamicPartitionPruning.enabled</code></td>
    <td>true</td>
    <td>3.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.optimizer.enableCsvExpressionOptimization</code></td>
    <td>true</td>
    <td>3.2.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.optimizer.enableJsonExpressionOptimization</code></td>
    <td>true</td>
    <td>3.1.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.optimizer.excludedRules</code></td>
    <td>(none)</td>
    <td>2.4.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.optimizer.runtime.bloomFilter.applicationSideScanSizeThreshold</code></td>
    <td>10GB</td>
    <td>3.3.0</td>
    <td>🚫</td>
</tr>

<tr>
    <td><code>spark.sql.optimizer.runtime.bloomFilter.creationSideThreshold</code></td>
    <td>10MB</td>
    <td>3.3.0</td>
    <td>🚫</td>
</tr>

<tr>
    <td><code>spark.sql.optimizer.runtime.bloomFilter.enabled</code></td>
    <td>true</td>
    <td>3.3.0</td>
    <td>✅</td>
</tr>

<tr>
    <td><code>spark.sql.optimizer.runtime.bloomFilter.expectedNumItems</code></td>
    <td>1000000</td>
    <td>3.3.0</td>
    <td>✅</td>
</tr>

<tr>
    <td><code>spark.sql.optimizer.runtime.bloomFilter.maxNumBits</code></td>
    <td>67108864</td>
    <td>3.3.0</td>
    <td>✅</td>
</tr>

<tr>
    <td><code>spark.sql.optimizer.runtime.bloomFilter.maxNumItems</code></td>
    <td>4000000</td>
    <td>3.3.0</td>
    <td>✅</td>
</tr>

<tr>
    <td><code>spark.sql.optimizer.runtime.bloomFilter.numBits</code></td>
    <td>8388608</td>
    <td>3.3.0</td>
    <td>✅</td>
</tr>

<tr>
    <td><code>spark.sql.optimizer.runtime.rowLevelOperationGroupFilter.enabled</code></td>
    <td>true</td>
    <td>3.4.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.optimizer.runtimeFilter.number.threshold</code></td>
    <td>10</td>
    <td>3.3.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.orc.aggregatePushdown</code></td>
    <td>false</td>
    <td>3.3.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.orc.columnarReaderBatchSize</code></td>
    <td>4096</td>
    <td>2.4.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.orc.columnarWriterBatchSize</code></td>
    <td>1024</td>
    <td>3.4.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.orc.compression.codec</code></td>
    <td>zstd</td>
    <td>2.3.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.orc.enableNestedColumnVectorizedReader</code></td>
    <td>true</td>
    <td>3.2.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.orc.enableVectorizedReader</code></td>
    <td>true</td>
    <td>2.3.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.orc.filterPushdown</code></td>
    <td>true</td>
    <td>1.4.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.orc.mergeSchema</code></td>
    <td>false</td>
    <td>3.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.orderByOrdinal</code></td>
    <td>true</td>
    <td>2.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.parquet.aggregatePushdown</code></td>
    <td>false</td>
    <td>3.3.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.parquet.binaryAsString</code></td>
    <td>false</td>
    <td>1.1.1</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.parquet.columnarReaderBatchSize</code></td>
    <td>4096</td>
    <td>2.4.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.parquet.compression.codec</code></td>
    <td>snappy</td>
    <td>1.1.1</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.parquet.enableNestedColumnVectorizedReader</code></td>
    <td>true</td>
    <td>3.3.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.parquet.enableVectorizedReader</code></td>
    <td>true</td>
    <td>2.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.parquet.fieldId.read.enabled</code></td>
    <td>false</td>
    <td>3.3.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.parquet.fieldId.read.ignoreMissing</code></td>
    <td>false</td>
    <td>3.3.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.parquet.fieldId.write.enabled</code></td>
    <td>true</td>
    <td>3.3.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.parquet.filterPushdown</code></td>
    <td>true</td>
    <td>1.2.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.parquet.inferTimestampNTZ.enabled</code></td>
    <td>true</td>
    <td>3.4.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.parquet.int96AsTimestamp</code></td>
    <td>true</td>
    <td>1.3.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.parquet.int96TimestampConversion</code></td>
    <td>false</td>
    <td>2.3.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.parquet.mergeSchema</code></td>
    <td>false</td>
    <td>1.5.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.parquet.outputTimestampType</code></td>
    <td>INT96</td>
    <td>2.3.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.parquet.recordLevelFilter.enabled</code></td>
    <td>false</td>
    <td>2.3.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.parquet.respectSummaryFiles</code></td>
    <td>false</td>
    <td>1.5.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.parquet.writeLegacyFormat</code></td>
    <td>false</td>
    <td>1.6.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.parser.quotedRegexColumnNames</code></td>
    <td>false</td>
    <td>2.3.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.pivotMaxValues</code></td>
    <td>10000</td>
    <td>1.6.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.planner.pythonExecution.memory</code></td>
    <td>(none)</td>
    <td>4.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.preserveCharVarcharTypeInfo</code></td>
    <td>false</td>
    <td>4.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.pyspark.inferNestedDictAsStruct.enabled</code></td>
    <td>false</td>
    <td>3.3.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.pyspark.jvmStacktrace.enabled</code></td>
    <td>false</td>
    <td>3.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.pyspark.plotting.max_rows</code></td>
    <td>1000</td>
    <td>4.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.pyspark.udf.profiler</code></td>
    <td>(none)</td>
    <td>4.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.readSideCharPadding</code></td>
    <td>true</td>
    <td>3.4.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.redaction.options.regex</code></td>
    <td>(?i)url</td>
    <td>2.2.2</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.redaction.string.regex</code></td>
    <td>(value of <code>spark.redaction.string.regex</code>)</td>
    <td>2.3.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.repl.eagerEval.enabled</code></td>
    <td>false</td>
    <td>2.4.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.repl.eagerEval.maxNumRows</code></td>
    <td>20</td>
    <td>2.4.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.repl.eagerEval.truncate</code></td>
    <td>20</td>
    <td>2.4.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.scripting.enabled</code></td>
    <td>false</td>
    <td>4.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.session.localRelationCacheThreshold</code></td>
    <td>67108864</td>
    <td>3.5.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.session.timeZone</code></td>
    <td>(value of local timezone)</td>
    <td>2.2.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.shuffle.partitions</code></td>
    <td>200</td>
    <td>1.1.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.shuffleDependency.fileCleanup.enabled</code></td>
    <td>false</td>
    <td>4.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.shuffleDependency.skipMigration.enabled</code></td>
    <td>false</td>
    <td>4.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.shuffledHashJoinFactor</code></td>
    <td>3</td>
    <td>3.3.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.sources.bucketing.autoBucketedScan.enabled</code></td>
    <td>true</td>
    <td>3.1.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.sources.bucketing.enabled</code></td>
    <td>true</td>
    <td>2.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.sources.bucketing.maxBuckets</code></td>
    <td>100000</td>
    <td>2.4.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.sources.default</code></td>
    <td>parquet</td>
    <td>1.3.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.sources.parallelPartitionDiscovery.threshold</code></td>
    <td>32</td>
    <td>1.5.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.sources.partitionColumnTypeInference.enabled</code></td>
    <td>true</td>
    <td>1.5.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.sources.partitionOverwriteMode</code></td>
    <td>STATIC</td>
    <td>2.3.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.sources.v2.bucketing.allowCompatibleTransforms.enabled</code></td>
    <td>false</td>
    <td>4.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.sources.v2.bucketing.allowJoinKeysSubsetOfPartitionKeys.enabled</code></td>
    <td>false</td>
    <td>4.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.sources.v2.bucketing.enabled</code></td>
    <td>false</td>
    <td>3.3.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.sources.v2.bucketing.partiallyClusteredDistribution.enabled</code></td>
    <td>false</td>
    <td>3.4.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.sources.v2.bucketing.partition.filter.enabled</code></td>
    <td>false</td>
    <td>4.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.sources.v2.bucketing.pushPartValues.enabled</code></td>
    <td>true</td>
    <td>3.4.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.sources.v2.bucketing.shuffle.enabled</code></td>
    <td>false</td>
    <td>4.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.sources.v2.bucketing.sorting.enabled</code></td>
    <td>false</td>
    <td>4.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.stackTracesInDataFrameContext</code></td>
    <td>1</td>
    <td>4.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.statistics.fallBackToHdfs</code></td>
    <td>false</td>
    <td>2.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.statistics.histogram.enabled</code></td>
    <td>false</td>
    <td>2.3.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.statistics.size.autoUpdate.enabled</code></td>
    <td>false</td>
    <td>2.3.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.statistics.updatePartitionStatsInAnalyzeTable.enabled</code></td>
    <td>false</td>
    <td>4.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.storeAssignmentPolicy</code></td>
    <td>ANSI</td>
    <td>3.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.streaming.checkpointLocation</code></td>
    <td>(none)</td>
    <td>2.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.streaming.continuous.epochBacklogQueueSize</code></td>
    <td>10000</td>
    <td>3.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.streaming.disabledV2Writers</code></td>
    <td></td>
    <td>2.3.1</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.streaming.fileSource.cleaner.numThreads</code></td>
    <td>1</td>
    <td>3.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.streaming.forceDeleteTempCheckpointLocation</code></td>
    <td>false</td>
    <td>3.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.streaming.metricsEnabled</code></td>
    <td>false</td>
    <td>2.0.2</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.streaming.multipleWatermarkPolicy</code></td>
    <td>min</td>
    <td>2.4.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.streaming.noDataMicroBatches.enabled</code></td>
    <td>true</td>
    <td>2.4.1</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.streaming.numRecentProgressUpdates</code></td>
    <td>100</td>
    <td>2.1.1</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.streaming.sessionWindow.merge.sessions.in.local.partition</code></td>
    <td>false</td>
    <td>3.2.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.streaming.stateStore.encodingFormat</code></td>
    <td>unsaferow</td>
    <td>4.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.streaming.stateStore.stateSchemaCheck</code></td>
    <td>true</td>
    <td>3.1.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.streaming.stopActiveRunOnRestart</code></td>
    <td>true</td>
    <td>3.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.streaming.stopTimeout</code></td>
    <td>0</td>
    <td>3.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.streaming.transformWithState.stateSchemaVersion</code></td>
    <td>3</td>
    <td>4.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.thriftServer.interruptOnCancel</code></td>
    <td>(value of <code>spark.sql.execution.interruptOnCancel</code>)</td>
    <td>3.2.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.thriftServer.queryTimeout</code></td>
    <td>0ms</td>
    <td>3.1.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.thriftserver.scheduler.pool</code></td>
    <td>(none)</td>
    <td>1.1.1</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.thriftserver.ui.retainedSessions</code></td>
    <td>200</td>
    <td>1.4.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.thriftserver.ui.retainedStatements</code></td>
    <td>200</td>
    <td>1.4.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.timeTravelTimestampKey</code></td>
    <td>timestampAsOf</td>
    <td>4.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.timeTravelVersionKey</code></td>
    <td>versionAsOf</td>
    <td>4.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.timestampType</code></td>
    <td>TIMESTAMP_LTZ</td>
    <td>3.4.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.transposeMaxValues</code></td>
    <td>500</td>
    <td>4.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.tvf.allowMultipleTableArguments.enabled</code></td>
    <td>false</td>
    <td>3.5.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.ui.explainMode</code></td>
    <td>formatted</td>
    <td>3.1.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.variable.substitute</code></td>
    <td>true</td>
    <td>2.0.0</td>
    <td></td>
</tr>
</table>


## Static SQL Configuration

<table class="spark-config">
<thead><tr><th>Property Name</th><th>Default</th><th>Since Version</th><th>Gluten Status</th></tr></thead>

<tr>
    <td><code>spark.sql.cache.serializer</code></td>
    <td>org.apache.spark.sql.execution.columnar.DefaultCachedBatchSerializer</td>
    <td>3.1.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.catalog.spark_catalog.defaultDatabase</code></td>
    <td>default</td>
    <td>3.4.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.event.truncate.length</code></td>
    <td>2147483647</td>
    <td>3.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.extensions</code></td>
    <td>(none)</td>
    <td>2.2.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.extensions.test.loadFromCp</code></td>
    <td>true</td>
    <td></td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.hive.metastore.barrierPrefixes</code></td>
    <td></td>
    <td>1.4.0</td>
    <td></td>
</tr>

<tr>
   <td><code>spark.sql.hive.metastore.jars</code></td>
   <td>builtin</td>
   <td>1.4.0</td>
   <td></td>
</tr>

<tr>
   <td><code>spark.sql.hive.metastore.jars.path</code></td>
   <td></td>
   <td>3.1.0</td>
   <td></td>
</tr>

<tr>
    <td><code>spark.sql.hive.metastore.sharedPrefixes</code></td>
    <td>com.mysql.jdbc,org.postgresql,com.microsoft.sqlserver,oracle.jdbc</td>
    <td>1.4.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.hive.metastore.version</code></td>
    <td>2.3.10</td>
    <td>1.4.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.hive.thriftServer.singleSession</code></td>
    <td>false</td>
    <td>1.6.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.hive.version</code></td>
    <td>2.3.10</td>
    <td>1.1.1</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.metadataCacheTTLSeconds</code></td>
    <td>-1000ms</td>
    <td>3.1.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.queryExecutionListeners</code></td>
    <td>(none)</td>
    <td>2.3.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.sources.disabledJdbcConnProviderList</code></td>
    <td></td>
    <td>3.1.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.streaming.streamingQueryListeners</code></td>
    <td>(none)</td>
    <td>2.4.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.streaming.ui.enabled</code></td>
    <td>true</td>
    <td>3.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.streaming.ui.retainedProgressUpdates</code></td>
    <td>100</td>
    <td>3.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.streaming.ui.retainedQueries</code></td>
    <td>100</td>
    <td>3.0.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.ui.retainedExecutions</code></td>
    <td>1000</td>
    <td>1.5.0</td>
    <td></td>
</tr>

<tr>
    <td><code>spark.sql.warehouse.dir</code></td>
    <td>(value of <code>$PWD/spark-warehouse</code>)</td>
    <td>2.0.0</td>
    <td></td>
</tr>
</table>

# Cluster Managers
These configurations are handled by Spark and do not affect Gluten’s behavior.

# Push-based shuffle overview

## External Shuffle service(server) side configuration options

<table class="spark-config">
<thead><tr><th>Property Name</th><th>Default</th><th>Since Version</th><th>Gluten Status</th></tr></thead>
<tr>
  <td><code>spark.shuffle.push.server.mergedShuffleFileManagerImpl</code></td>
  <td>
    <code>org.apache.spark.network.shuffle.<br />NoOpMergedShuffleFileManager</code>
  </td>
  <td>3.2.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.shuffle.push.server.minChunkSizeInMergedShuffleFile</code></td>
  <td><code>2m</code></td>
  <td>3.2.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.shuffle.push.server.mergedIndexCacheSize</code></td>
  <td><code>100m</code></td>
  <td>3.2.0</td>
  <td></td>
</tr>
</table>

### Client side configuration options

<table class="spark-config">
<thead><tr><th>Property Name</th><th>Default</th><th>Since Version</th><th>Gluten Status</th></tr></thead>
<tr>
  <td><code>spark.shuffle.push.enabled</code></td>
  <td><code>false</code></td>
  <td>3.2.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.shuffle.push.finalize.timeout</code></td>
  <td><code>10s</code></td>
  <td>3.2.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.shuffle.push.maxRetainedMergerLocations</code></td>
  <td><code>500</code></td>
  <td>3.2.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.shuffle.push.mergersMinThresholdRatio</code></td>
  <td><code>0.05</code></td>
  <td>3.2.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.shuffle.push.mergersMinStaticThreshold</code></td>
  <td><code>5</code></td>
  <td>3.2.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.shuffle.push.numPushThreads</code></td>
  <td>(none)</td>
  <td>3.2.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.shuffle.push.maxBlockSizeToPush</code></td>
  <td><code>1m</code></td>
  <td>3.2.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.shuffle.push.maxBlockBatchSize</code></td>
  <td><code>3m</code></td>
  <td>3.2.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.shuffle.push.merge.finalizeThreads</code></td>
  <td>8</td>
  <td>3.3.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.shuffle.push.minShuffleSizeToWait</code></td>
  <td><code>500m</code></td>
  <td>3.3.0</td>
  <td></td>
</tr>
<tr>
  <td><code>spark.shuffle.push.minCompletedPushRatio</code></td>
  <td><code>1.0</code></td>
  <td>3.3.0</td>
  <td></td>
</tr>
</table>






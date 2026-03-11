layout: page
title: Parquet write configuration
nav_order: 17

## Parquet write configurations in Spark/Velox/Gluten

Gluten configuration includes two types. config in parquet and config in spark. the two configurations below has the same effect. One is for spark session, the other is for the query.

```
sc.conf.set("spark.gluten.sql.native.parquet.write.blockRows")

df.write.option("parquet.block.rows").save()

```


<table class="spark-config">
<thead>
	<tr>
		<th></th>
		<th>parquet-mr default</th>
		<th>Spark default</th>
		<th>Velox Default</th>
		<th>Gluten Config</th>
	</tr>
</thead>
<tbody>
	<tr>
		<td colspan="5" align="center"> 
      <hr/>Spark <hr/></td>
	</tr>
	<tr>
		<td><code>spark.sql.parquet.outputTimestampType</code></td>
		<td></td><td>int96</td><td></td><td></td>
	</tr>
	<tr>
		<td><code>spark.sql.parquet.writeLegacyFormat</code></td>
		<td></td><td>false</td><td></td><td></td>
	</tr>
	<tr>
		<td colspan="5" align="center"> <hr/>Velox/Arrow <hr/></td>
	</tr>
	<tr>
		<td><code>write_batch_size</code></td>
		<td></td><td></td><td>1024</td><td>spark.gluten.sql.columnar.maxBatchSize</td>
	</tr>
	<tr>
		<td><code>rowgroup_length</code></td>
		<td></td><td></td><td>1M</td><td>parquet.block.rows<br>spark.gluten.sql.native.parquet.write.blockRows</td>
	</tr>
	<tr>
		<td><code>compression_level</code></td>
		<td></td><td></td><td>0</td><td></td>
	</tr>
	<tr>
		<td><code>page_index</code></td>
		<td></td><td></td><td>false</td><td></td>
	</tr>
	<tr>
		<td><code>decimal_as_integer</code></td>
		<td></td><td></td><td>false</td><td></td>
	</tr>
	<tr>
		<td><code>statistics_enabled</code></td>
		<td></td><td></td><td>false</td><td></td>
	</tr>
	<tr>
		<td  colspan="5" align="center"> <hr/>parquet-mr <hr/></td>
	</tr>
	<tr>
		<td><code>parquet.summary.metadata.level</code></td>
		<td>all</td><td></td><td></td><td></td>
	</tr>
	<tr>
		<td><code>parquet.enable.summary-metadata</code></td>
		<td>true</td><td></td><td></td><td></td>
	</tr>
	<tr>
		<td><code>parquet.block.size</code></td>
		<td>128m</td><td></td><td></td><td>parquet.block.size<br>spark.gluten.sql.columnar.parquet.write.blockSize</td>
	</tr>
	<tr>
		<td><code>parquet.page.size</code></td>
		<td>1m</td><td></td><td>1M</td><td>parquet.page.size</td>
	</tr>
	<tr>
		<td><code>parquet.compression</code></td>
		<td>uncompressed</td><td>snappy</td><td>uncompressed</td><td>parquet.compression<br>spark.sql.parquet.compression.codec</td>
	</tr>
	<tr>
		<td><code>parquet.write.support.class</code></td>
		<td>org.apache.parquet<br>.hadoop.api.WriteSupport</td><td></td><td></td><td></td>
	</tr>
	<tr>
		<td><code>parquet.enable.dictionary</code></td>
		<td>true</td><td></td><td>true</td><td>parquet.enable.dictionary</td>
	</tr>
	<tr>
		<td><code>parquet.dictionary.page.size</code></td>
		<td>1m</td><td></td><td>1m</td><td></td>
	</tr>
	<tr>
		<td><code>parquet.validation</code></td>
		<td>false</td><td></td><td></td><td></td>
	</tr>
	<tr>
		<td><code>parquet.writer.version</code></td>
		<td>PARQUET_1_0</td><td></td><td>PARQUET_2_6</td><td>parquet.writer.version</td>
	</tr>
	<tr>
		<td><code>parquet.memory.pool.ratio</code></td>
		<td>0.95</td><td></td><td></td><td></td>
	</tr>
	<tr>
		<td><code>parquet.memory.min.chunk.size</code></td>
		<td>1m</td><td></td><td></td><td></td>
	</tr>
	<tr>
		<td><code>parquet.writer.max-padding</code></td>
		<td>8m</td><td></td><td></td><td></td>
	</tr>
	<tr>
		<td><code>parquet.page.size.row.check.min</code></td>
		<td>100</td><td></td><td></td><td></td>
	</tr>
	<tr>
		<td><code>parquet.page.size.row.check.max</code></td>
		<td>10000</td><td></td><td></td><td></td>
	</tr>
	<tr>
		<td><code>parquet.page.value.count.threshold</code></td>
		<td>Integer.MAX_VALUE / 2</td><td></td><td></td><td></td>
	</tr>
	<tr>
		<td><code>parquet.page.size.check.estimate</code></td>
		<td>true</td><td></td><td></td><td></td>
	</tr>
	<tr>
		<td><code>parquet.columnindex.truncate.length</code></td>
		<td>64</td><td></td><td></td><td></td>
	</tr>
	<tr>
		<td><code>parquet.statistics.truncate.length</code></td>
		<td>2147483647</td><td></td><td></td><td></td>
	</tr>
	<tr>
		<td><code>parquet.bloom.filter.enabled</code></td>
		<td>false</td><td></td><td></td><td></td>
	</tr>
	<tr>
		<td><code>parquet.bloom.filter.adaptive.enabled</code></td>
		<td>false</td><td></td><td></td><td></td>
	</tr>
	<tr>
		<td><code>parquet.bloom.filter.candidates.number</code></td>
		<td>5</td><td></td><td></td><td></td>
	</tr>
	<tr>
		<td><code>parquet.bloom.filter.expected.ndv</code></td>
		<td></td><td></td><td></td><td></td>
	</tr>
	<tr>
		<td><code>parquet.bloom.filter.fpp</code></td>
		<td>0.01</td><td></td><td></td><td></td>
	</tr>
	<tr>
		<td><code>parquet.bloom.filter.max.bytes</code></td>
		<td>1m</td><td></td><td></td><td></td>
	</tr>
	<tr>
		<td><code>parquet.decrypt.off-heap.buffer.enabled</code></td>
		<td>false</td><td></td><td></td><td></td>
	</tr>
	<tr>
		<td><code>parquet.page.row.count.limit</code></td>
		<td>20000</td><td></td><td></td><td></td>
	</tr>
	<tr>
		<td><code>parquet.page.write-checksum.enabled</code></td>
		<td>true</td><td></td><td>false</td><td></td>
	</tr>
	<tr>
		<td><code>parquet.crypto.factory.class</code></td>
		<td>None</td><td></td><td></td><td></td>
	</tr>
	<tr>
		<td><code>parquet.compression.codec.zstd.bufferPool.enabled</code></td>
		<td>true</td><td></td><td></td><td></td>
	</tr>
	<tr>
		<td><code>parquet.compression.codec.zstd.level</code></td>
		<td>3</td><td></td><td>0</td><td>parquet.compression.codec.zstd.level</td>
	</tr>
	<tr>
		<td><code>parquet.compression.codec.zstd.workers</code></td>
		<td>0</td><td></td><td></td><td></td>
	</tr>
</tbody>
</table>

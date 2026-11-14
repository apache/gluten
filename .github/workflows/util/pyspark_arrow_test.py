# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Runs PySpark's Arrow conversions (createDataFrame(pandas_df) and toPandas()) with the Gluten
# bundle on the classpath, to check that Spark's own Arrow code still works next to the Arrow
# classes Gluten bundles. Submit it with the bundle in spark.{driver,executor}.extraClassPath and
# spark.plugins=org.apache.gluten.GlutenPlugin, see the pyspark-arrow-test job in
# velox_backend_x86.yml.

import numpy as np
import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("pyspark-arrow-test").getOrCreate()
assert "org.apache.gluten.GlutenPlugin" in spark.conf.get("spark.plugins", ""), \
    "Gluten is not enabled, submit with spark.plugins=org.apache.gluten.GlutenPlugin"

spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", "false")

pdf = pd.DataFrame(np.full((100, 3), 5))

df = spark.createDataFrame(pdf)
result_pdf = df.select("*").toPandas()
print("Pandas DataFrame result statics:\n%s\n" % str(result_pdf.describe()))
assert result_pdf.shape == (100, 3), "Unexpected shape %s" % str(result_pdf.shape)
assert (result_pdf.to_numpy() == 5).all(), "Unexpected values in the result"

spark.stop()
print("SUCCESS: createDataFrame + toPandas completed with Arrow and the Gluten bundle")

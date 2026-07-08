---
layout: page
title: Gluten for Flink Troubleshooting
---
## Troubleshooting

### How to make sure gluten classes loaded first in Flink?

Gluten classes need to be loaded first in Flink,
you can modify the constructFlinkClassPath function in `$FLINK_HOME/bin/config.sh` like this:

```
GLUTEN_JAR="$FLINK_HOME/gluten_lib/gluten-flink-loader-1.8.0-SNAPSHOT.jar:$FLINK_HOME/gluten_lib/velox4j-0.1.0-SNAPSHOT.jar:$FLINK_HOME/gluten_lib/gluten-flink-runtime-1.8.0-SNAPSHOT.jar:"
echo "$GLUTEN_JAR""$FLINK_CLASSPATH""$FLINK_DIST"
```

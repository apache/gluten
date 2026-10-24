# encode

Native support is checked after the same Spark expression preparation used by the JVM.

| case | inputs | expression | description |
| --- | --- | --- | --- |
| standard-string | `input = standard.string(length = 10)` | `encode(input, 'UTF-8')` | Prepared UTF-8 encoding; skipped at runtime if the Native converter does not support the expanded expression. |

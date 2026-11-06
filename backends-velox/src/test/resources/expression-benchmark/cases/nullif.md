# nullif

| case | inputs | expression | description |
| --- | --- | --- | --- |
| standard-long | `input = standard.long()` | `nullif(input, CAST(17 AS BIGINT))` | Calibration report R:750; projected standard inputs from CalibrationDataGenerator. |
| computed-long | `input = standard.long()` | `nullif(input + 1, 17L)` | Preserve the standard multi-project plan. |
| random-result | `none` | `nullif(rand(7), 0.5D)` | Preserve nondeterministic provenance across projects. |

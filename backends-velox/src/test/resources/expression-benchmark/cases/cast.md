# cast

| case | inputs | expression | description |
| --- | --- | --- | --- |
| long-to-string | `input = standard.long()` | `CAST(input AS STRING)` | Calibration report R:256; projected standard inputs from CalibrationDataGenerator. |
| long-to-decimal | `input = standard.long()` | `CAST(input AS DECIMAL(20,0))` | Decimal output regression using standard long inputs without overflow. |
| string-to-long | `input = standard.string(length = 10)` | `CAST(input AS BIGINT)` | Calibration report R:261; projected standard inputs from CalibrationDataGenerator. |

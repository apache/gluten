# concat

| case | inputs | expression | description |
| --- | --- | --- | --- |
| int-array | `input = standard.intArray()` | `concat(input, array(1))` | Calibration report R:387; projected standard inputs from CalibrationDataGenerator. |
| string-array | `input = standard.stringArray(length = 10)` | `concat(input, array('fixed'))` | Calibration report R:392; projected standard inputs from CalibrationDataGenerator. |
| string-two | `input = standard.string(length = 10)` | `concat(input, 'fixed')` | Calibration report R:397; projected standard inputs from CalibrationDataGenerator. |
| string-three | `input = standard.string(length = 10)` | `concat(input, 'fixed', input)` | Calibration report R:402; projected standard inputs from CalibrationDataGenerator. |
| binary | `input = standard.binary()` | `concat(input, CAST('fixed' AS BINARY))` | Calibration report R:407; projected standard inputs from CalibrationDataGenerator. |

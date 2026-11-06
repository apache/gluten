# map_from_arrays

| case | inputs | expression | description |
| --- | --- | --- | --- |
| standard-string-int | `keys = standard.stringArray(length = 10); value = standard.int()` | `map_from_arrays(keys, array(value, value + 1))` | Calibration report R:758; projected standard inputs from CalibrationDataGenerator. |

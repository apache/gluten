# make_date

| case | inputs | expression | description |
| --- | --- | --- | --- |
| bounded-int | `input = standard.boundedInt()` | `make_date(2020, 1, (input % 20) + 1)` | Calibration report R:781; projected standard inputs from CalibrationDataGenerator. |

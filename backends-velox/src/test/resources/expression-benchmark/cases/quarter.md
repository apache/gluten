# quarter

| case | inputs | expression | description |
| --- | --- | --- | --- |
| standard-date | `input = standard.date()` | `quarter(input)` | Calibration report R:862; projected standard inputs from CalibrationDataGenerator. |
| quarter-boundaries | `input = quarterInput(mode="quarter-boundaries")` | `quarter(input)` | Prepared quarter-boundaries DATE inputs; no parsing or casts in timed expression. |
| leap-centuries | `input = quarterInput(mode="leap-centuries")` | `quarter(input)` | Prepared leap-centuries DATE inputs; no parsing or casts in timed expression. |
| pre-epoch | `input = quarterInput(mode="pre-epoch")` | `quarter(input)` | Prepared pre-epoch DATE inputs; no parsing or casts in timed expression. |
| wide-range | `input = quarterInput(mode="wide-range")` | `quarter(input)` | Prepared wide-range DATE inputs; no parsing or casts in timed expression. |
| nullable | `input = quarterInput(mode="nullable")` | `quarter(input)` | Prepared nullable DATE inputs; no parsing or casts in timed expression. |

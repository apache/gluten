# rand

| case | inputs | expression | description |
| --- | --- | --- | --- |
| seeded | `none` | `rand(7)` | Explicit seed without input columns; engine result values are not compared. |
| unseeded | `none` | `rand()` | Common analysis resolves the seed once; engines keep independent RNG state and result values are not compared. |

CREATE TABLE nexmark_q7 (
  auction  BIGINT,
  bidder  BIGINT,
  price  BIGINT,
  `dateTime`  TIMESTAMP(3),
  extra  VARCHAR
) WITH (
  'connector' = 'blackhole'
);

INSERT INTO nexmark_q7
SELECT B.auction, B.price, B.bidder, B.`dateTime`, B.extra
from bid B
JOIN (
  SELECT MAX(price) AS maxprice, window_end as `dateTime`
  FROM TABLE(
          TUMBLE(TABLE bid, DESCRIPTOR(`dateTime`), INTERVAL '10' SECOND))
  GROUP BY window_start, window_end
) B1
ON B.price = B1.maxprice
WHERE B.`dateTime` BETWEEN B1.`dateTime`  - INTERVAL '10' SECOND AND B1.`dateTime`;

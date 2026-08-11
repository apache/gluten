CREATE TABLE nexmark_q10_orc (
  auction  BIGINT,
  bidder  BIGINT,
  price  BIGINT,
  `dateTime`  TIMESTAMP(3),
  extra  VARCHAR,
  dt STRING,
  hm STRING
) PARTITIONED BY (dt, hm) WITH (
  'connector' = 'filesystem',
  'path' = 'file:///tmp/data/output/bid_orc/',
  'format' = 'orc',
  'sink.partition-commit.trigger' = 'process-time',
  'sink.partition-commit.delay' = '0s',
  'sink.partition-commit.policy.kind' = 'success-file',
  'partition.time-extractor.timestamp-pattern' = '$dt $hm:00',
  'sink.rolling-policy.rollover-interval' = '1s',
  'sink.rolling-policy.check-interval' = '1s'
);

INSERT INTO nexmark_q10_orc
SELECT auction, bidder, price, `dateTime`, extra, DATE_FORMAT(`dateTime`, 'yyyy-MM-dd'), DATE_FORMAT(`dateTime`, 'HH:mm')
FROM bid;

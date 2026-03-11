CREATE TABLE nexmark_q10 (
  auction  BIGINT,
  bidder  BIGINT,
  price  BIGINT,
  `dateTime`  TIMESTAMP(3),
  extra  VARCHAR,
  dt STRING,
  hm STRING
) PARTITIONED BY (dt, hm) WITH (
  'connector' = 'filesystem',
  'path' = 'file:///tmp/data/output/bid/',
  'format' = 'csv',
  'sink.partition-commit.trigger' = 'partition-time',
  'sink.partition-commit.delay' = '1 min',
  'sink.partition-commit.policy.kind' = 'success-file',
  'partition.time-extractor.timestamp-pattern' = '$dt $hm:00',
  'sink.rolling-policy.rollover-interval' = '1min',
  'sink.rolling-policy.check-interval' = '1min'
);

INSERT INTO nexmark_q10
SELECT auction, bidder, price, `dateTime`, extra, DATE_FORMAT(`dateTime`, 'yyyy-MM-dd'), DATE_FORMAT(`dateTime`, 'HH:mm')
FROM bid;

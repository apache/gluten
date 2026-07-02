CREATE TABLE nexmark_q12 (
  bidder BIGINT,
  bid_count BIGINT,
  starttime TIMESTAMP(3),
  endtime TIMESTAMP(3)
) WITH (
  'connector' = 'blackhole'
);

CREATE VIEW B AS SELECT *, PROCTIME() as p_time FROM bid;

INSERT INTO nexmark_q12
SELECT
    bidder,
    count(*) as bid_count,
    window_start AS starttime,
    window_end AS endtime
FROM TABLE(
        TUMBLE(TABLE B, DESCRIPTOR(p_time), INTERVAL '10' SECOND))
GROUP BY bidder, window_start, window_end;

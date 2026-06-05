CREATE TABLE nexmark_q5 (
  auction  BIGINT,
  num  BIGINT
) WITH (
  'connector' = 'blackhole'
);

INSERT INTO nexmark_q5
SELECT AuctionBids.auction, AuctionBids.num
 FROM (
   SELECT
     auction,
     count(*) AS num,
     window_start AS starttime,
     window_end AS endtime
     FROM TABLE(
             HOP(TABLE bid, DESCRIPTOR(`dateTime`), INTERVAL '2' SECOND, INTERVAL '10' SECOND))
     GROUP BY auction, window_start, window_end
 ) AS AuctionBids
 JOIN (
   SELECT
     max(CountBids.num) AS maxn,
     CountBids.starttime,
     CountBids.endtime
   FROM (
     SELECT
       count(*) AS num,
       window_start AS starttime,
       window_end AS endtime
     FROM TABLE(
                HOP(TABLE bid, DESCRIPTOR(`dateTime`), INTERVAL '2' SECOND, INTERVAL '10' SECOND))
     GROUP BY auction, window_start, window_end
     ) AS CountBids
   GROUP BY CountBids.starttime, CountBids.endtime
 ) AS MaxBids
 ON AuctionBids.starttime = MaxBids.starttime AND
    AuctionBids.endtime = MaxBids.endtime AND
    AuctionBids.num >= MaxBids.maxn;

-- NOTE: Groupby Operator is currently not fully stable,
--       we are actively working on stabilizing it.

-- Emit Page view counts collected grouped by page key in the last
-- 5 minutes at 5 minute interval and send the result to a kafka topic.
-- Using GetSqlField UDF to extract page key from the requestHeader.
insert into kafka.groupbyTopic
  select GetSqlField(pv.requestHeader) as __key__, GetPageKey(pv.requestHeader) as pageKey, count(*) as Views
  from kafka.`PageViewEvent` as pv
  group by GetSqlField(pv.requestHeader)

-- You can add additional SQL statements here

-- NOTE: Join Operator is currently not fully stable,
--       we are actively working on stabilizing it.

-- Enrich PageViewEvent with member profile data
INSERT INTO kafka.tracking.EnrichedPageVIewEvent
SELECT *
FROM Kafka.PageViewEvent as pv
  JOIN Kafka.ProfileChanges.`$table` as p
    ON pv.memberid = p.memberid

-- You can add additional SQL statements here

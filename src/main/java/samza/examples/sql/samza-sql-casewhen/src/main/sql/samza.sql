-- For each profile in Kafka Profile change capture stream, identify whether the
-- profile is a quality profile or not and insert the result into QualityProfile
-- kafka topic. Please note the usage of GetSqlField UDF to extract the company
-- name field from nested record.

INSERT INTO kafka.QualityProfile
SELECT id, status, case when (profilePicture <> null and industryName <> null and
GetSqlField(positions, 'Position.companyName') <> null)
then 1 else 0 end as quality
FROM kafka.ProfileChanges

-- you can add additional SQL statements here

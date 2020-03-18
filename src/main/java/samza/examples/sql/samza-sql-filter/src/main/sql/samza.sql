-- Filter Profile change-capture stream by 'Product Manager'
-- title and project basic profile data to a kafka topic.

INSERT INTO kafka.ProductManagerProfiles
SELECT memberId, firstName, lastName, company
FROM kafka.ProfileChanges
WHERE standardize(title) = 'Product Manager'

-- you can add additional SQL statements here

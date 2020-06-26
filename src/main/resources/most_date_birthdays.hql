-- Calculate top three days of birth dates
WITH a AS (
    SELECT CONCAT(SPLIT(birth_date, '-')[1], "-", SPLIT(birth_date, '-')[2]) AS month_day
    FROM birthday
)

SELECT month_day, COUNT(*) AS birth_freq
FROM a
WHERE month_day IS NOT NULL
GROUP BY month_day
ORDER BY birth_freq DESC, month_day ASC
LIMIT 3;

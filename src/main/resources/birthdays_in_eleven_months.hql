SELECT id, birth_date,
  CONCAT(SPLIT(birth_date, '-')[1], "-", SPLIT(birth_date, '-')[2]) AS month_day
FROM birthday
WHERE CAST(datediff(add_months(to_date(from_unixtime(unix_timestamp())), 11), birth_date)/365.25 AS INT)
 - CAST(datediff(to_date(from_unixtime(unix_timestamp())), birth_date)/365.25 AS INT) <> 0
ORDER BY month_day

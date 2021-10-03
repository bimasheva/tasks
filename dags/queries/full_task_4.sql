WITH
  weekly_data AS (
  SELECT
    DATE_TRUNC(DATE(created_utc), WEEK(Monday)) pt,
    COUNT(subr) num_subreddits
  FROM
    `fh-bigquery.reddit.subreddits`
  GROUP BY
    pt )
SELECT
  *,
  CASE
    WHEN LAG(pt) OVER(ORDER BY pt) = DATE_SUB(pt, INTERVAL 7 day) THEN num_subreddits - IFNULL(LAG(num_subreddits) OVER(ORDER BY pt), 0)
  ELSE
  num_subreddits
END
  AS wow_change
FROM
  weekly_data
ORDER BY
  pt;
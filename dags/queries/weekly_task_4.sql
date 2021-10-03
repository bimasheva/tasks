WITH
  prev_weekly_data AS (
  SELECT
    COUNT(subr) prev_num_subreddits
  FROM
    `fh-bigquery.reddit.subreddits`
  WHERE
    DATE(created_utc) >= '{{ prev_ds }}'
    AND DATE(created_utc) < '{{ ds }}'
  GROUP BY
    pt )
SELECT
  '{{ ds }}' AS pt,
  COUNT(subr) num_subreddits,
  COUNT(subr) - (
  SELECT
    prev_num_subreddits
  FROM
    prev_weekly_data) AS wow_change
FROM
  `fh-bigquery.reddit.subreddits`
WHERE
  DATE(created_utc) >= '{{ ds }}'
  AND DATE(created_utc) < '{{ next_ds }}'
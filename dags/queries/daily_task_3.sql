SELECT
  '{{ ds }}' pt,
  subr,
  num_comments
FROM
  `fh-bigquery.reddit.subreddits`
WHERE
  DATE(created_utc) = '{{ ds }}'
ORDER BY
  num_comments DESC
LIMIT
  1;

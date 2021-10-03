SELECT
  '{{ ds }}' AS dt,
  SUM(num_comments) num_comments,
  SUM(c_posts) AS posts,
  SUM(ups) AS ups,
  SUM(downs) AS downs,
  ARRAY_AGG(STRUCT(subr,
      num_comments,
      c_posts AS posts,
      ups,
      downs)) AS subreddit_metrics
FROM
  `fh-bigquery.reddit.subreddits`
WHERE
  DATE(created_utc) = '{{ ds }}';
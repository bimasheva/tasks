WITH
  result AS (
  SELECT
    DATE(created_utc) pt,
    subr,
    num_comments,
    RANK() OVER(PARTITION BY DATE(created_utc)
    ORDER BY
      num_comments DESC) rn
  FROM
    `fh-bigquery.reddit.subreddits` )
SELECT
  * EXCEPT(rn)
FROM
  result
WHERE
  rn = 1;
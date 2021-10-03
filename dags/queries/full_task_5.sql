WITH
  top_cnts AS (
  SELECT
    COUNT(DISTINCT subr)*0.25 top_cnt
  FROM
    `fh-bigquery.reddit.subreddits` ),
  ranked_data AS (
  SELECT
    *,
    ups/(ups+downs) up_ratio,
    RANK() OVER(ORDER BY ups/(ups+downs) DESC) ranks
  FROM
    `fh-bigquery.reddit.subreddits` )
SELECT
  *EXCEPT(ranks),
  CASE
    WHEN ranks >= top_cnt THEN 1
  ELSE
  0
END
  top_p75_upvote
FROM
  ranked_data
LEFT JOIN
  top_cnts
ON
  1=1;
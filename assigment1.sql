--SQL script to get the data from Stack exchange
SELECT top(50000)
  Id AS id,
  Title AS post_title,
  Score AS score,
  Body AS post_text,
  ViewCount AS view_count,
--source: https://stackoverflow.com/questions/13402732/sql-server-elegant-way-to-check-if-value-is-null-or-integer-value
--source:https://stackoverflow.com/questions/951518/replace-a-newline-in-tsql
  ISNULL(CONVERT(VARCHAR(20), OwnerUserId), REPLACE(REPLACE(REPLACE(OwnerDisplayName, CHAR(9), ''), CHAR(11), ''), CHAR(32), '') ) AS user_id
FROM posts
WHERE
  posts.ViewCount > 100000

  --posts.ViewCount < 127403 and
  --posts.ViewCount < 74597 and
  --posts.ViewCount < 53212 and
  --posts.ViewCount < 41326 and

  --posts.ViewCount > 10000
ORDER BY posts.ViewCount DESC;

WITH "ReplyCount" AS (
    SELECT
        post.reply_root AS uri,
        COUNT(*) AS score
    FROM "Post" as post
    INNER JOIN "Actor" as author on post."authorId" = author.did
      AND post.reply_root IS NOT NULL
      AND post.indexed_at > (:current_time - interval '20 hours')
      AND post.indexed_at < :current_time
      AND NOT author.is_muted
      AND author.manual_include_in_fox_feed IS NOT FALSE
    GROUP BY post.reply_root
), "QuoteCount" AS (
    SELECT
        post.embed_uri AS uri,
        COUNT(*) AS score
    FROM "Post" as post
    INNER JOIN "Actor" as author on post."authorId" = author.did
      AND post.embed_uri IS NOT NULL
      AND post.indexed_at > (:current_time - interval '20 hours')
      AND post.indexed_at < :current_time
      AND NOT author.is_muted
      AND author.manual_include_in_fox_feed IS NOT FALSE
    GROUP BY post.embed_uri
), "Everything" AS (
    SELECT * FROM "ReplyCount"
    UNION ALL SELECT * FROM "QuoteCount"
)

SELECT
    uri,
    SUM(score) AS score
FROM "Everything"
GROUP BY uri
ORDER BY score DESC
LIMIT 500

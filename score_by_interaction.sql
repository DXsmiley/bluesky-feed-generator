WITH "ReplyCount" AS (
    SELECT
        post.reply_root AS uri,
        (3 * COUNT(*)) AS positive,
        0 as negative
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
        (4 * COUNT(*)) AS positive,
        0 AS negative
    FROM "Post" as post
    INNER JOIN "Actor" as author on post."authorId" = author.did
      AND post.embed_uri IS NOT NULL
      AND post.indexed_at > (:current_time - interval '20 hours')
      AND post.indexed_at < :current_time
      AND NOT author.is_muted
      AND author.manual_include_in_fox_feed IS NOT FALSE
    GROUP BY post.embed_uri
), "NotLikes" AS (
    SELECT
        lk.post_uri AS uri,
        0 AS positive,
        COUNT(*) AS negative
    FROM "Like" as lk
    WHERE lk.created_at < :current_time
    GROUP BY lk.post_uri
), "NotLikes2" AS (
    SELECT
        post.uri as post,
        0 AS positive,
        post.like_count AS negative
    FROM "Post" as post
    WHERE post.indexed_at < :current_time
), "Concated" AS (
    SELECT * FROM "ReplyCount"
    UNION ALL SELECT * FROM "QuoteCount"
    UNION ALL SELECT * FROM "NotLikes"
    UNION ALL SELECT * FROM "NotLikes2"
), "Everything" AS (
    SELECT
        uri,
        SUM(positive) AS positive,
        SUM(negative) AS negative
    FROM "Concated"
    GROUP BY uri
)

SELECT
    uri,
    (positive - negative) AS score
FROM "Everything"
WHERE positive + negative > 30
  AND positive - negative > 10
ORDER BY score DESC
LIMIT 500

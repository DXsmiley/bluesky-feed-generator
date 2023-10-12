WITH t AS (
    SELECT p1.embed_uri AS uri FROM "Post" as p1 LEFT OUTER JOIN "Post" as p2 ON p1.embed_uri = p2.uri WHERE p2.uri IS NULL AND p1.embed_uri LIKE '%/app.bsky.feed.post/%'
    UNION SELECT p1.reply_root AS uri FROM "Post" as p1 LEFT OUTER JOIN "Post" as p2 ON p1.reply_root = p2.uri WHERE p2.uri IS NULL AND p1.reply_root LIKE '%/app.bsky.feed.post/%'
    UNION SELECT p1.reply_parent AS uri FROM "Post" as p1 LEFT OUTER JOIN "Post" as p2 ON p1.reply_parent = p2.uri WHERE p2.uri IS NULL AND p1.reply_parent LIKE '%/app.bsky.feed.post/%'
)

SELECT * FROM t LIMIT 1000;

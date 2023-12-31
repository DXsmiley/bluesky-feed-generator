WITH t AS (
    SELECT p1.embed_uri AS uri
    FROM "Post" as p1
    LEFT OUTER JOIN "Post" as p2 ON p1.embed_uri = p2.uri
    LEFT OUTER JOIN "Actor" as author ON p1."authorId" = author.did
    WHERE p2.uri IS NULL
      AND p1.embed_uri LIKE '%/app.bsky.feed.post/%'
      AND author.is_external_to_network IS FALSE
    UNION SELECT p1.reply_root AS uri
    FROM "Post" as p1
    LEFT OUTER JOIN "Post" as p2 ON p1.reply_root = p2.uri
    LEFT OUTER JOIN "Actor" as author ON p1."authorId" = author.did
    WHERE p2.uri IS NULL
      AND p1.reply_root LIKE '%/app.bsky.feed.post/%'
      AND author.is_external_to_network IS FALSE
    UNION SELECT p1.reply_parent AS uri
    FROM "Post" as p1
    LEFT OUTER JOIN "Post" as p2 ON p1.reply_parent = p2.uri
    LEFT OUTER JOIN "Actor" as author ON p1."authorId" = author.did
    WHERE p2.uri IS NULL
      AND p1.reply_parent LIKE '%/app.bsky.feed.post/%'
      AND author.is_external_to_network IS FALSE
)

SELECT * FROM t LIMIT 1000;


# This is kinda weird and really bad sorry

from datetime import datetime
import foxfeed.database
from typing import List, Union

Arg = Union[str, int, float, bool, datetime]

def escape(a: Arg) -> str:
    if isinstance(a, bool):
        return 'TRUE' if a else 'FALSE'
    if isinstance(a, str):
        assert "'" not in a
        return "'" + a + "'"
    if isinstance(a, int):
        return str(a)
    if isinstance(a, float):
        return str(a)
    if isinstance(a, datetime):
        return "'" + a.isoformat().split('.')[0] + "'::timestamp"

score_posts_sql_query = """
WITH "LikeCount" AS (
    -- Splitting this out seems to give performance improvements over doing the
    -- count inside table1
    SELECT
        lk.post_uri,
        COUNT(*) AS count
    FROM "Like" as lk
    INNER JOIN "Actor" as liker ON lk.liker_id = liker.did
    AND lk.created_at > ({current_time} - interval '96 hours')
    AND lk.created_at < {current_time}
    AND NOT liker.is_muted
    AND liker.manual_include_in_fox_feed IS NOT FALSE
    AND liker.is_external_to_network IS FALSE
    AND (
        {include_guy_votes}
        OR liker.manual_include_in_vix_feed IS TRUE
            OR (
                liker.manual_include_in_vix_feed IS NOT FALSE
                AND liker.autolabel_fem_vibes IS TRUE
                AND liker.autolabel_masc_vibes IS FALSE
            )
        )
    GROUP BY lk.post_uri
), table1 AS (
    SELECT
        post.uri AS uri,
        post.embed_uri AS embed_uri,
        post."authorId" AS author,
        post.indexed_at AS indexed_at,
        post.labels AS labels,
        (
            EXTRACT(EPOCH FROM ({current_time} - post.indexed_at)) /
            EXTRACT(EPOCH FROM interval {beta})
        ) AS x,
        (
            (CASE WHEN post.media_count > 0 AND post.media_with_alt_text_count = 0 THEN 0.7 ELSE 1.0 END)
            -- An attempt to stop a few large accounts dominating the feed
            -- This is bad because it creates a way for people to de-rank others intentionally
            -- Also low-key breaks generating old snapshots
            * (0.7 + (-0.1 * ATAN(author.follower_count / 800)))
        ) AS multiplier,
        (
            like_count.count
        ) AS likes,
        (
            author.manual_include_in_vix_feed IS TRUE
            OR (
                author.manual_include_in_vix_feed IS NOT FALSE
                AND author.autolabel_fem_vibes IS TRUE
                AND author.autolabel_masc_vibes IS FALSE
            )
        ) AS author_is_fem
    FROM "Post" as post
    INNER JOIN "Actor" as author on post."authorId" = author.did
    INNER JOIN "LikeCount" as like_count on post.uri = like_count.post_uri
    WHERE post.indexed_at > ({current_time} - interval '96 hours')
        AND post.indexed_at < {current_time}
        AND post.is_deleted IS FALSE
        AND post.reply_root IS NULL
        -- Pinned posts get mixed into the feed in a different way, so exclude them from scoring
        AND NOT post.is_pinned
        AND NOT author.is_muted
        AND author.manual_include_in_fox_feed IS NOT FALSE
        AND author.is_external_to_network IS FALSE
), table2 AS (
    SELECT
        uri,
        embed_uri,
        author,
        author_is_fem,
        indexed_at,
        labels,
        (
            (
                CASE WHEN {do_time_decay}
                THEN (CASE WHEN x > 1 THEN (1 / POWER(x, {alpha})) ELSE (2 - (1 / POWER((2 - x), {alpha}))) END)
                ELSE 1
                END 
            )
            * multiplier
            * (POWER(likes, {gamma}) + 2)
        ) AS score
    FROM table1 as post
    WHERE {include_guy_posts} OR author_is_fem
    -- Not required but this seems to give performance improvements?
    ORDER BY author, score DESC
), table3 AS (
    SELECT
        uri,
        author,
        author_is_fem,
        indexed_at,
        labels,
        (
            score
            * (1 / POWER(2, RANK() OVER (PARTITION BY author ORDER BY score DESC) - 1))
            * (
                CASE WHEN embed_uri IS NULL THEN 1
                ELSE (1 / POWER(2, RANK() OVER (PARTITION BY embed_uri ORDER BY score DESC) - 1)) END
            )
        ) AS score
    FROM table2
)

SELECT * FROM table3 ORDER BY score DESC LIMIT {lmt};

"""

async def score_posts(
    db: foxfeed.database.Database,
    *,
    alpha: Arg,
    beta: Arg,
    current_time: Arg,
    do_time_decay: Arg,
    gamma: Arg,
    include_guy_posts: Arg,
    include_guy_votes: Arg,
    lmt: Arg,
) -> List[foxfeed.database.ScorePostsOutputModel]:
    query = score_posts_sql_query.format(
        alpha = escape(alpha),
        beta = escape(beta),
        current_time = escape(current_time),
        do_time_decay = escape(do_time_decay),
        gamma = escape(gamma),
        include_guy_posts = escape(include_guy_posts),
        include_guy_votes = escape(include_guy_votes),
        lmt = escape(lmt),
    )
    result = await db.query_raw(query, model=foxfeed.database.ScorePostsOutputModel) # type: ignore
    return result

score_by_interactions_sql_query = """
WITH "ReplyCount" AS (
    SELECT
        post.reply_root AS uri,
        COUNT(*) AS positive,
        0 as negative
    FROM "Post" as post
    INNER JOIN "Actor" as author on post."authorId" = author.did
      AND post.reply_root IS NOT NULL
      AND post.indexed_at > ({current_time} - interval '20 hours')
      AND post.indexed_at < {current_time}
      AND NOT author.is_muted
      AND author.manual_include_in_fox_feed IS NOT FALSE
    GROUP BY post.reply_root
), "QuoteCount" AS (
    SELECT
        post.embed_uri AS uri,
        (3 * COUNT(*)) AS positive,
        0 AS negative
    FROM "Post" as post
    INNER JOIN "Actor" as author on post."authorId" = author.did
      AND post.embed_uri IS NOT NULL
      AND post.indexed_at > ({current_time} - interval '20 hours')
      AND post.indexed_at < {current_time}
      AND NOT author.is_muted
      AND author.manual_include_in_fox_feed IS NOT FALSE
    GROUP BY post.embed_uri
), "Concated" AS (
    SELECT * FROM "ReplyCount"
    UNION ALL SELECT * FROM "QuoteCount"
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
WHERE positive - negative > 4
ORDER BY score DESC
LIMIT 500

"""

async def score_by_interactions(
    db: foxfeed.database.Database,
    *,
    current_time: Arg,
) -> List[foxfeed.database.ScoreByInteractionOutputModel]:
    query = score_by_interactions_sql_query.format(
        current_time = escape(current_time),
    )
    result = await db.query_raw(query, model=foxfeed.database.ScoreByInteractionOutputModel) # type: ignore
    return result

find_unlinks_sql_query = """
WITH t AS (
    SELECT p1.embed_uri AS uri FROM "Post" as p1 LEFT OUTER JOIN "Post" as p2 ON p1.embed_uri = p2.uri WHERE p2.uri IS NULL AND p1.embed_uri LIKE '%/app.bsky.feed.post/%'
    UNION SELECT p1.reply_root AS uri FROM "Post" as p1 LEFT OUTER JOIN "Post" as p2 ON p1.reply_root = p2.uri WHERE p2.uri IS NULL AND p1.reply_root LIKE '%/app.bsky.feed.post/%'
    UNION SELECT p1.reply_parent AS uri FROM "Post" as p1 LEFT OUTER JOIN "Post" as p2 ON p1.reply_parent = p2.uri WHERE p2.uri IS NULL AND p1.reply_parent LIKE '%/app.bsky.feed.post/%'
)

SELECT * FROM t LIMIT 1000;

"""

async def find_unlinks(
    db: foxfeed.database.Database,
) -> List[foxfeed.database.FindUnlinksOutputModel]:
    query = find_unlinks_sql_query.format(
    )
    result = await db.query_raw(query, model=foxfeed.database.FindUnlinksOutputModel) # type: ignore
    return result

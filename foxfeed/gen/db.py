
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
        post."authorId" as author,
        post.indexed_at as indexed_at,
        post.labels as labels,
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
            -- Funny :)
            * (0.95 + (0.05 * SIN(like_count.count * 20)))
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
        -- Pinned posts get mixed into the feed in a different way, so exclude them from scoring
        AND NOT post.is_pinned
        AND NOT author.is_muted
        AND author.manual_include_in_fox_feed IS NOT FALSE
), table2 AS (
    SELECT
        uri,
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
        (score * (1 / POWER(2, RANK() OVER (PARTITION BY author ORDER BY score DESC) - 1))) AS score
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
    result = await db.query_raw(query, model=foxfeed.database.ScorePostsOutputModel)
    return result

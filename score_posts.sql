WITH table1 AS (
    SELECT
        post.uri AS uri,
        post."authorId" as author,
        post.indexed_at as indexed_at,
        post.labels as labels,
        (
            EXTRACT(EPOCH FROM (NOW() - post.indexed_at)) /
            EXTRACT(EPOCH FROM interval :beta)
        ) AS x,
        (
            (CASE WHEN post.media_count > 0 AND post.media_with_alt_text_count = 0 THEN 0.7 ELSE 1.0 END)
        ) AS multiplier,
        (
            SELECT COUNT(*)
            FROM "Like" as lk
            INNER JOIN "Actor" as liker ON lk.liker_id = liker.did
            WHERE lk.post_uri = post.uri
            AND lk.created_at > NOW() - interval '96 hours'
            AND NOT liker.is_muted
            AND liker.manual_include_in_fox_feed IS NOT FALSE
            AND (
                :include_guy_votes
                OR liker.manual_include_in_vix_feed IS TRUE
                    OR (
                        liker.manual_include_in_vix_feed IS NOT FALSE
                        AND liker.autolabel_fem_vibes IS TRUE
                        AND liker.autolabel_masc_vibes IS FALSE
                    )
                )
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
    WHERE post.indexed_at > NOW() - interval '96 hours'
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
                CASE WHEN :do_time_decay
                THEN (CASE WHEN x > 1 THEN (1 / POWER(x, :alpha)) ELSE (2 - (1 / POWER((2 - x), :alpha))) END)
                ELSE 1
                END 
            )
            * multiplier
            * (POWER(likes, :gamma) + 2)
        ) AS score
    FROM table1 as post
    WHERE :include_guy_posts OR author_is_fem
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

SELECT * FROM table3 ORDER BY score DESC LIMIT :lmt;

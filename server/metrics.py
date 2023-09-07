import math
from server.database import Database
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import Literal, Iterator, List


@dataclass
class FeedMetricsSlice:
    start: datetime
    end: datetime
    feed_name: str
    attributed_likes: int
    num_requests: int
    posts_served: int
    unique_viewers: int


@dataclass
class FeedMetrics:
    start: datetime
    end: datetime
    feed_name: str
    timesliced: List[FeedMetricsSlice]


async def feed_metrics_for_timeslice(
    db: Database,
    feed_name: str,
    start: datetime,
    end: datetime,
) -> FeedMetricsSlice:
    attributed_likes = await db.like.count(
        where={
            "attributed_feed": {"endswith": feed_name},
            "created_at": {"gte": start, "lte": end},
        }
    )
    num_requests = await db.servedblock.count(
        where={
            "feed_name": {"endswith": feed_name},
            "when": {"gte": start, "lte": end},
        }
    )
    posts_served = await db.servedpost.count(
        where={
            "feed_name": {"endswith": feed_name},
            "when": {"gte": start, "lte": end},
        }
    )
    unique_viewers = len(
        await db.servedblock.find_many(
            where={
                "feed_name": {"endswith": feed_name},
                "when": {"gte": start, "lte": end},
            },
            distinct=["client_did"],
        )
    )
    return FeedMetricsSlice(
        start=start,
        end=end,
        feed_name=feed_name,
        attributed_likes=attributed_likes,
        num_requests=num_requests,
        posts_served=posts_served,
        unique_viewers=unique_viewers,
    )


async def feed_metrics_for_time_range(
    db: Database,
    feed_name: str,
    start: datetime,
    end: datetime,
    interval: timedelta,
    *,
    floor_start_and_end: Literal[True] = True
) -> FeedMetrics:
    f_start = floor_datetime(start, interval)
    metrics = [
        await feed_metrics_for_timeslice(db, feed_name, i, i + interval)
        for i in daterange(f_start, end, interval)
    ]
    return FeedMetrics(
        start=f_start,
        end=f_start + interval * len(metrics),
        feed_name=feed_name,
        timesliced=metrics,
    )


def floor_datetime(dt: datetime, interval: timedelta) -> datetime:
    f: float = (dt - datetime.min) / interval
    return datetime.min + math.floor(f) * interval


def daterange(
    start: datetime, end: datetime, interval: timedelta
) -> Iterator[datetime]:
    c = start
    while c < end:
        yield c
        c = c + interval

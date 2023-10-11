import asyncio
import gc
from typing import List, Set
from datetime import datetime, timedelta, timezone
import traceback

from termcolor import cprint

import prisma.types
import prisma.errors

from foxfeed.bsky import AsyncClient, get_specific_posts
from foxfeed.database import Database
from foxfeed.store import store_post2
from foxfeed.algos.feeds import algo_details
from foxfeed.algos.generators import RunDetails
from foxfeed.util import sleep_on


async def refresh_posts(
        db: Database,
        client: AsyncClient,
        all_uris_in_feeds: List[str],
        run_endtime: datetime
):
    posts_to_refresh = await db.post.find_many(
        take=500,
        where={
            'uri': {'in': list(all_uris_in_feeds)},
            'indexed_at': {'lt': run_endtime - timedelta(minutes=20)},
            'reply_root': None,
            'OR': [
                {'last_rescan': None},
                {'last_rescan': {'lt': run_endtime - timedelta(hours=6)}},
            ]
        }
    )
    if posts_to_refresh:
        print(f'Refreshing {len(posts_to_refresh)} posts')
        async for i in get_specific_posts(client, [i.uri for i in posts_to_refresh]):
            await store_post2(db, i, None, None, now=run_endtime)
        print('Refresh done')


async def score_posts(shutdown_event: asyncio.Event, db: Database, client: AsyncClient, do_refresh_posts: bool = False) -> None:
    run_starttime = datetime.now(tz=timezone.utc)
    run_version = int(run_starttime.timestamp())

    cprint(f"Starting scoring round {run_version}", "yellow", force_color=True)

    rd = RunDetails(
        run_starttime=run_starttime, run_version=run_version
    )

    all_uris_in_feeds: Set[str] = set()
    for algo in algo_details:
        gen = algo['generator']
        if gen is not None:
            result = await gen(db, rd)
            await save_generator_results(db, algo['record_name'], rd, result)
            all_uris_in_feeds |= set(result)
        if shutdown_event.is_set():
            break

    run_endtime = datetime.now(tz=timezone.utc)

    cprint(
        f"Scoring round {run_version} took {(run_endtime - run_starttime).seconds // 60} minutes",
        "yellow",
        force_color=True,
    )

    if shutdown_event.is_set():
        cprint("It ended early due to a shutdown event", "yellow", force_color=True)
        return

    await db.postscore.delete_many(
        where={"created_at": {"lt": run_starttime - timedelta(hours=1)}}
    )

    if do_refresh_posts:
        await refresh_posts(db, client, list(all_uris_in_feeds), run_endtime)


async def save_generator_results(db: Database, feed_name: str, rd: RunDetails, will_store: List[str]) -> None:
    blob: List[prisma.types.PostScoreCreateWithoutRelationsInput] = [
        {
            "uri": post_uri,
            "version": rd.run_version,
            # This is actually "rank"
            "score": len(will_store) - i,
            "created_at": rd.run_starttime,
            "feed_name": feed_name,
        }
        for i, post_uri in enumerate(will_store)
    ]

    try:
        await db.postscore.create_many(data=blob)
    except prisma.errors.UniqueViolationError:
        cprint(f"Unique violation when creating PostScores on {feed_name}::{rd.run_version}", 'red', force_color=True)



async def score_posts_forever(shutdown_event: asyncio.Event, db: Database, client: AsyncClient, forever: bool):
    if forever:
        while not shutdown_event.is_set():
            try:
                await score_posts(shutdown_event, db, client, do_refresh_posts=True)
                cprint(f"gc-d {gc.collect()} objects", "yellow", force_color=True)
            except Exception:
                cprint(f"Error during score_posts", color="red", force_color=True)
                traceback.print_exc()
            await sleep_on(shutdown_event, 60)
    else:
        await score_posts(shutdown_event, db, client, do_refresh_posts=True)

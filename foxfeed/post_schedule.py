import asyncio
from datetime import datetime, timedelta
import dateutil.tz
from typing import Optional

from foxfeed import config
from foxfeed.bsky import AsyncClient
from foxfeed.database import Database
from foxfeed.util import sleep_on

import traceback


AEST = dateutil.tz.tzoffset('AEST', timedelta(hours=10))


async def step_schedule(db: Database, client: AsyncClient, shutdown_event: asyncio.Event) -> Optional[timedelta]:
    now = datetime.now(tz=AEST)
    timespan_start = now.replace(hour=17, minute=30, second=0, microsecond=0) # 5:30 pm
    timespan_end = now.replace(hour=22, minute=0, second=0, microsecond=0) # 10:00 pm
    print(now, timespan_start, timespan_end)
    if now < timespan_start:
        print(f'Sleeping from {now} until {timespan_start} (witing for start of timespan)')
        return timespan_start - now
    if now > timespan_end:
        next_timespan_start = timespan_start + timedelta(days=1)
        print(f'Sleeping from {now} until {next_timespan_start} (waiting for end of timespan)')
        return next_timespan_start - now
    post_age_amount = timedelta(hours=8)
    recent_post = await db.post.find_first(
        order={'indexed_at': 'desc'},
        where={
            'author': {'is': {'handle': config.HANDLE}},
            'indexed_at': {'gt': now - post_age_amount},
        }
    )
    if recent_post is not None:
        until_post_is_old = recent_post.indexed_at + post_age_amount
        print(f'Sleeping from {now} until {until_post_is_old} (waiting for posts to age)')
        return until_post_is_old - now
    next_post = await db.scheduledpost.find_first(
        order={'id': 'asc'},
        where={'status': 'scheduled'}
    )
    if next_post is None:
        print('There are no posts to schedule, sleeping for a bit')
        return timedelta(minutes=10)
    await db.scheduledpost.update(where={'id': next_post.id}, data={'status': 'attempting'})
    try:
        print('Posting:', next_post.text)
        result = await client.send_post(text=next_post.text)
        print('Done!')
    except Exception:
        print('Failed to post the post')
        traceback.print_exc()
        await db.scheduledpost.update(where={'id': next_post.id}, data={'status': 'failed'})
    else:
        await db.scheduledpost.update(where={'id': next_post.id}, data={'status': 'posted', 'post_uri': result.uri})
    finally:
        return timedelta(minutes=10)


async def run_schedule(db: Database, client: AsyncClient, shutdown_event: asyncio.Event, run_forever: bool):
    if run_forever:
        # Wait a bit for the firehose to catch up and so we don't immediately post shit
        await sleep_on(shutdown_event, 60 * 5)
        while not shutdown_event.is_set():
            sleep_for = await step_schedule(db, client, shutdown_event)
            if sleep_for is not None:
                await sleep_on(shutdown_event, sleep_for.total_seconds())
    else:
        await step_schedule(db, client, shutdown_event)

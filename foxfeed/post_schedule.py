import asyncio
from datetime import datetime, timedelta, timezone
from typing import Optional, AsyncIterator
from prisma.models import ScheduledPost
from backports.zoneinfo import ZoneInfo

from foxfeed.bsky import AsyncClient
from foxfeed.database import Database
from foxfeed.util import sleep_on, parse_datetime, alist
from atproto.xrpc_client import models

import re
import traceback


POST_COOLDOWN = timedelta(hours=8)
IMAGE_POST_COOLDOWN = timedelta(hours=40)


async def find_facets(client: AsyncClient, data: bytes) -> AsyncIterator[models.AppBskyRichtextFacet.Main]:
    mention = rb'@([a-zA-Z0-9-_]+\.)+[a-zA-Z0-9-_]+'
    for item in re.finditer(mention, data):
        handle = item.group(0).decode('utf-8').replace('@', '')
        user = await client.app.bsky.actor.get_profile({'actor': handle})
        yield models.AppBskyRichtextFacet.Main(
            features=[
                models.AppBskyRichtextFacet.Mention(did=user.did)
            ],
            index=models.AppBskyRichtextFacet.ByteSlice(
                byteStart=item.start(0),
                byteEnd=item.end(0),
            ),
        )


async def send_post(client: AsyncClient, post: ScheduledPost) -> models.ComAtprotoRepoCreateRecord.Response:
    assert client.me is not None
    images = (
        None if not post.media
        else models.AppBskyEmbedImages.Main(
            images=[
                models.AppBskyEmbedImages.Image(
                    alt=image.alt_text,
                    image=(await client.com.atproto.repo.upload_blob(image.data.decode(), timeout=30)).blob
                )
                for image in post.media
            ]
        )
    )
    labels = (
        None if not post.label
        else models.ComAtprotoLabelDefs.SelfLabels(
            values=[
                models.ComAtprotoLabelDefs.SelfLabel(
                    val=post.label
                )
            ]
        )
    )
    facets = await alist(find_facets(client, post.text.encode('utf-8')))
    record = models.AppBskyFeedPost.Main(
        createdAt = client.get_current_time_iso(),
        text = post.text,
        reply = None,
        embed = images,
        langs = ['en'],
        labels = labels,
        facets = facets,
    )
    return await client.com.atproto.repo.create_record(
        models.ComAtprotoRepoCreateRecord.Data(
            repo = client.me.did,
            collection = models.ids.AppBskyFeedPost,
            record = record,
        )
    )


async def step_schedule(db: Database, client: AsyncClient) -> Optional[timedelta]:
    assert client.me is not None
    handle = client.me.handle
    now = datetime.now(tz=ZoneInfo('Australia/Sydney'))
    timespan_start = now.replace(hour=16, minute=0, second=0, microsecond=0) # 6:00 pm
    timespan_end = now.replace(hour=22, minute=0, second=0, microsecond=0) # 10:00 pm
    print(now, timespan_start, timespan_end)
    if now < timespan_start:
        print(f'Sleeping from {now} until {timespan_start} (waiting for start of timespan)')
        return timespan_start - now
    if now > timespan_end:
        next_timespan_start = timespan_start + timedelta(days=1)
        print(f'Sleeping from {now} until {next_timespan_start} (waiting for end of timespan)')
        return next_timespan_start - now
    recent_posts_on_bsky = (await client.app.bsky.feed.get_author_feed({'actor': handle})).feed
    if recent_posts_on_bsky:
        posted_at = parse_datetime(recent_posts_on_bsky[0].post.indexed_at).astimezone(tz=timezone.utc)
        until_post_is_old = posted_at + POST_COOLDOWN
        print(f'Sleeping from {now} until {until_post_is_old} (waiting for posts to age, got from bsky directly)')
        if now < until_post_is_old:
            return until_post_is_old - now
    recent_post = await db.post.find_first(
        order={'indexed_at': 'desc'},
        where={
            'author': {'is': {'handle': handle}},
            'indexed_at': {'gt': now - POST_COOLDOWN},
            'reply_root': None,
        }
    )
    if recent_post is not None:
        until_post_is_old = recent_post.indexed_at + POST_COOLDOWN
        print(f'Sleeping from {now} until {until_post_is_old} (waiting for posts to age, got from db)')
        return until_post_is_old - now
    recent_image_post = await db.post.find_first(
        order={'indexed_at': 'desc'},
        where={
            'author': {'is': {'handle': handle}},
            'indexed_at': {'gt': now - IMAGE_POST_COOLDOWN},
            'media_count': {'gt': 0},
            'reply_root': None,
        }
    )
    next_post = None
    if recent_image_post is None:
        print('There is no recent image post, gonna see if theres a post with an image')
        next_post = await db.scheduledpost.find_first(
            order={'id': 'desc'},
            where={
                'status': 'scheduled',
                'media': {'some': {}},
                'scheduled_at': {'lt': now - timedelta(hours=1)},
            },
            include={'media': True},
        )
    if next_post is None:
        print('Gonna see if there\'s a text-only post')
        next_post = await db.scheduledpost.find_first(
            order={'id': 'desc'},
            where={
                'status': 'scheduled',
                'media': {'none': {}},
                'scheduled_at': {'lt': now - timedelta(hours=1)},
            },
            include={'media': True},
        )
    if next_post is None:
        print('There are no posts to schedule, sleeping for a bit')
        return timedelta(minutes=10)
    await db.scheduledpost.update(where={'id': next_post.id}, data={'status': 'attempting'})
    try:
        print('Posting:', next_post.text)
        # result = await client.send_post(text=next_post.text)
        result = await send_post(client, next_post)
        print('Done!')
    except Exception:
        print('Failed to post the post')
        traceback.print_exc()
        await db.scheduledpost.update(where={'id': next_post.id}, data={'status': 'failed'})
    else:
        await db.scheduledpost.update(where={'id': next_post.id}, data={'status': 'posted', 'post_uri': result.uri})
    finally:
        return timedelta(minutes=10)


async def run_schedule(db: Database, client: AsyncClient, shutdown_event: asyncio.Event, run_forever: bool) -> None:
    if run_forever:
        # Wait a bit for the firehose to catch up and so we don't immediately post shit
        await sleep_on(shutdown_event, 60 * 5)
        while not shutdown_event.is_set():
            sleep_for = await step_schedule(db, client)
            if sleep_for is not None:
                await sleep_on(shutdown_event, sleep_for.total_seconds())
    else:
        await step_schedule(db, client)


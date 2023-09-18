from atproto.xrpc_client import models
from atproto.xrpc_client.models.utils import is_record_type

from server.logger import logger
from server.data_stream import OpsByType

from typing import List, Callable, Coroutine, Any
from prisma.types import PostCreateWithoutRelationsInput
import prisma.errors

from server.database import Database, care_about_storing_user_data_preemptively
from server.load_known_furries import parse_datetime

from server.util import mentions_fursuit, parse_datetime

from datetime import datetime, timedelta

from collections import OrderedDict

import time


class CachedQuery:

    def __init__(self, function: Callable[[Database, str], Coroutine[Any, Any, bool]]):
        self.function = function
        self.hits = 1
        self.misses = 1
        self.name = function.__name__
        self.cache: 'OrderedDict[str, bool]' = OrderedDict()
        self.last_log = time.time()

    async def __call__(self, db: Database, key: str) -> bool:
        curtime = time.time()
        if curtime - self.last_log > 60:
            self.log_stats()
            self.last_log = curtime
        if key in self.cache:
            self.hits += 1
            return self.cache[key]
        self.misses += 1
        result = await self.function(db, key)
        self.cache[key] = result
        while len(self.cache) > 5_000:
            self.cache.popitem(False)
        return result
    
    def log_stats(self):
        ratio = 100 * (self.hits / (self.hits + self.misses))
        print(f'Cache: {self.name} | {len(self.cache)} | {self.hits} {self.misses} | {ratio:.1f}%')


@CachedQuery
async def user_exists_cached(db: Database, did: str) -> bool:
    user = await db.actor.find_first(
        where={
            "did": did,
            "AND": [care_about_storing_user_data_preemptively],
        }
    )
    return user is not None


@CachedQuery
async def post_exists_cached(db: Database, uri: str) -> bool:
    return (await db.post.find_first(where={'uri': uri})) is not None



async def operations_callback(db: Database, ops: OpsByType) -> None:
    posts_to_create: List[PostCreateWithoutRelationsInput] = []

    for created_post in ops["posts"]["created"]:
        author_did = created_post["author"]
        record = created_post["record"]

        # if record.embed is not None:
        #     print(type(record.embed))
        #     print(record.embed)

        inlined_text = record.text.replace("\n", " ")
        images = (
            record.embed.images
            if record.embed is not None and is_record_type(record.embed, models.AppBskyEmbedImages)
            else []
        )
        images_with_alt_text = [i for i in images if (i.alt or '').strip() != ""]
        image_urls = {
            index: f"https://av-cdn.bsky.app/img/feed_thumbnail/plain/{author_did}/{image.image.ref}@jpeg"
            for index, image in enumerate(images)
        }

        reply_parent = None
        try:
            if record.reply and record.reply.parent.uri:
                reply_parent = record.reply.parent.uri
        except AttributeError:
            continue

        reply_root = None
        try:
            if record.reply and record.reply.root.uri:
                reply_root = record.reply.root.uri
        except AttributeError:
            continue

        labels = (
            []
            if not isinstance(record.labels, models.ComAtprotoLabelDefs.SelfLabels)
            else [i.val for i in record.labels.values]
        )

        # We're not doing anything with replies right now so we'll just ignore them to save cycles
        if reply_parent is not None or reply_root is not None:
            continue

        if await user_exists_cached(db, author_did):
            logger.info(
                f"New furry post (with images: {len(images)}, labels: {labels}): {inlined_text}"
            )
            post_dict: PostCreateWithoutRelationsInput = {
                "uri": created_post["uri"],
                "cid": created_post["cid"],
                "reply_parent": reply_parent,
                "reply_root": reply_root,
                "authorId": created_post["author"],
                "text": record.text,
                "mentions_fursuit": mentions_fursuit(record.text),
                "media_count": len(images),
                "media_with_alt_text_count": len(images_with_alt_text),
                "m0": image_urls.get(0, None),
                "m1": image_urls.get(1, None),
                "m2": image_urls.get(2, None),
                "m3": image_urls.get(3, None),
                "labels": labels,
            }
            posts_to_create.append(post_dict)

    if posts_to_create:
        await db.post.create_many(posts_to_create, skip_duplicates=True)

    posts_to_delete = [p["uri"] for p in ops["posts"]["deleted"]]
    if posts_to_delete:
        deleted_rows = await db.post.delete_many(where={"uri": {"in": posts_to_delete}})
        if deleted_rows:
            logger.info(f"Deleted from feed: {deleted_rows}")

    for like in ops["likes"]["created"]:
        uri = like["record"]["subject"]["uri"]

        # Placing user before post here results in a much better cache hit rate
        if not await user_exists_cached(db, like['author']):
            continue
        if not await post_exists_cached(db, uri):
            continue

        served_post = await db.servedpost.find_first(
            where={
                "when": {"gt": datetime.now() - timedelta(minutes=5)},
                "post_uri": like["record"].subject.uri,
                "client_did": like["author"],
            }
        )

        if served_post is None:
            print(f"Someone liked a post")
        else:
            print(f"Someone liked a post, attirbuted to", served_post.feed_name)

        try:
            await db.like.create(
                data={
                    "uri": like["uri"],
                    "cid": like["cid"],
                    "liker_id": like["author"],
                    "post_uri": like["record"].subject.uri,
                    "post_cid": like["record"].subject.cid,
                    "created_at": parse_datetime(like["record"].created_at),
                    "attributed_feed": None
                    if served_post is None
                    else served_post.feed_name,
                }
            )
        except prisma.errors.UniqueViolationError:
            pass

    # TODO: Handle deleted likes lmao

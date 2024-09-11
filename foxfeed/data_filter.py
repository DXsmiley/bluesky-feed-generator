import asyncio

from atproto import models
from foxfeed.util import is_record_type

from foxfeed.logger import logger
from foxfeed.firehose.data_stream import OpsByType

from typing import List, Callable, Coroutine, Any, Union, Dict, Tuple, TypeVar, Generic, Literal
from prisma.types import PostCreateWithoutRelationsInput, LikeCreateWithoutRelationsInput

from foxfeed.database import Database, care_about_storing_user_data_preemptively

from foxfeed.util import mentions_fursuit, parse_datetime

from datetime import datetime, timedelta

from collections import OrderedDict

import time


T = TypeVar('T')


class CachedQuery(Generic[T]):

    def __init__(self, function: Callable[[Database, str], Coroutine[Any, Any, T]]):
        self.function = function
        self.hits = 1
        self.misses = 1
        self.name = function.__name__
        self.cache: 'OrderedDict[str, T]' = OrderedDict()
        self.last_log = time.time()
        self.lock = None

    async def __call__(self, db: Database, key: str) -> T:
        if self.lock is None:
            self.lock = asyncio.Lock()
        async with self.lock:
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
        
    async def drop_entry(self, key: str) -> None:
        if self.lock is None:
            self.lock = asyncio.Lock()
        async with self.lock:
            self.cache.pop(key, None)
    
    def log_stats(self):
        ratio = 100 * (self.hits / (self.hits + self.misses))
        print(f'Cache: {self.name} | {len(self.cache)} | {self.hits} {self.misses} | {ratio:.1f}%')


@CachedQuery
async def user_exists_cached(db: Database, did: str) -> Literal['not-here', 'do-care', 'dont-care']:
    user = await db.actor.find_first(
        where={
            "did": did,
            "AND": [care_about_storing_user_data_preemptively],
        }
    )
    if user is None:
        return 'not-here'
    if (
        user.is_muted is False
        and user.manual_include_in_fox_feed is not False
        and user.is_external_to_network is False
    ):
        return 'do-care'
    return 'dont-care'


@CachedQuery
async def post_exists_cached(db: Database, uri: str) -> Literal['not-here', 'do-care', 'dont-care']:
    post = await db.post.find_first(where={'uri': uri}, include={'author': True})
    if post is None or post.author is None:
        return 'not-here'
    if (
        post.author.is_muted is False
        and post.author.manual_include_in_fox_feed is not False
        and post.author.is_external_to_network is False
    ):
        return 'do-care'
    return 'dont-care'


EmbedType = Union[
    None,
    'models.AppBskyEmbedImages.Main',
    'models.AppBskyEmbedVideo.Main',
    'models.AppBskyEmbedExternal.Main',
    'models.AppBskyEmbedRecord.Main',
    'models.AppBskyEmbedRecordWithMedia.Main',
]


def get_images(author_did: str, embed: EmbedType) -> Tuple[int, Dict[int, str]]:
    if is_record_type(embed, models.AppBskyEmbedRecordWithMedia):
        embed = embed.media
    if is_record_type(embed, models.AppBskyEmbedImages):
        num_with_alt_text = len([i for i in embed.images if (i.alt or '').strip() != ""])
        image_urls = {
            index: f"https://av-cdn.bsky.app/img/feed_thumbnail/plain/{author_did}/{image.image.ref}@jpeg"
            for index, image in enumerate(embed.images)
        }
        return (num_with_alt_text, image_urls)
    return (0, {})


def get_quoted_skeet(embed: EmbedType) -> Union[Tuple[str, str], Tuple[None, None]]:
    if is_record_type(embed, models.AppBskyEmbedRecordWithMedia):
        embed = embed.record
    if is_record_type(embed, models.AppBskyEmbedRecord):
        return (embed.record.uri, embed.record.cid)
    return (None, None)


async def operations_callback(db: Database, ops: OpsByType) -> None:
    posts_to_create: List[PostCreateWithoutRelationsInput] = []

    unknown_things_to_queue: List[Tuple[str, Literal['post', 'actor', 'like']]] = []

    for created_post in ops["posts"]["created"]:
        author_did = created_post["author"]
        record = created_post["record"]

        inlined_text = record.text.replace("\n", " ")
        
        num_with_alt_text, image_urls = get_images(author_did, record.embed)
        embed_uri, embed_cid = get_quoted_skeet(record.embed)

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

        # we must care about SOMETHING going on
        care_about_something_here = (
            await user_exists_cached(db, author_did) == 'do-care'
            or (reply_parent and await post_exists_cached(db, reply_parent) == 'do-care')
            or (reply_root and await post_exists_cached(db, reply_root) == 'do-care')
            or (embed_uri and await post_exists_cached(db, embed_uri) == 'do-care')
        )

        # all posts referenced must exist in the database
        # ideally we should branch out from here, however Prisma has some limitations about non-existent
        # references and we need to drop old posts to keep the DB under the row limit, so unfortunately these need to go
        post_links_exist = (
            (reply_parent and await post_exists_cached(db, reply_parent) != 'not-here')
            and (reply_root and await post_exists_cached(db, reply_root) != 'not-here')
            and (embed_uri and await post_exists_cached(db, embed_uri) != 'not-here')
        )

        if (not care_about_something_here) or (not post_links_exist):
            continue

        can_store_immediately = True

        if await user_exists_cached(db, author_did) == 'not-here':
            unknown_things_to_queue.append((author_did, 'actor'))
            can_store_immediately = False

        if reply_root and await post_exists_cached(db, reply_root) == 'not-here':
            unknown_things_to_queue.append((reply_root, 'post'))
            can_store_immediately = False

        if reply_parent and await post_exists_cached(db, reply_parent) == 'not-here':
            unknown_things_to_queue.append((reply_parent, 'post'))
            can_store_immediately = False

        if embed_uri and await post_exists_cached(db, embed_uri) == 'not-here':
            unknown_things_to_queue.append((embed_uri, 'post'))
            can_store_immediately = False

        # This is a reply to something we care about
        if not can_store_immediately:
            unknown_things_to_queue.append((created_post['uri'], 'post'))
        else:
            logger.info(
                f"New furry post (is: {embed_uri is not None}, reply: {reply_root is not None}, images: {len(image_urls)}, labels: {labels}): {inlined_text}"
            )
            post_dict: PostCreateWithoutRelationsInput = {
                "uri": created_post["uri"],
                "cid": created_post["cid"],
                "reply_parent": reply_parent,
                "reply_root": reply_root,
                "authorId": created_post["author"],
                "text": record.text,
                "mentions_fursuit": mentions_fursuit(record.text),
                "media_count": len(image_urls),
                "media_with_alt_text_count": num_with_alt_text,
                "m0": image_urls.get(0, None),
                "m1": image_urls.get(1, None),
                "m2": image_urls.get(2, None),
                "m3": image_urls.get(3, None),
                "labels": labels,
                "embed_uri": embed_uri,
                "embed_cid": embed_cid,
            }
            posts_to_create.append(post_dict)

    if posts_to_create:
        await db.post.create_many(posts_to_create, skip_duplicates=True)

    posts_to_delete = [p["uri"] for p in ops["posts"]["deleted"]]
    if posts_to_delete:
        deleted_rows = await db.post.update_many(
            where={"uri": {"in": posts_to_delete}},
            data={"is_deleted": True}
        )
        if deleted_rows:
            logger.info(f"Deleted from feed: {deleted_rows}")

    likes_to_create: List[LikeCreateWithoutRelationsInput] = []

    for like in ops["likes"]["created"]:
        uri = like["record"]["subject"]["uri"]

        # TODO: Store out-of-network likes

        care_about_something_here = (
            # Placing user before post here results in a much better cache hit rate
            await user_exists_cached(db, like['author']) == 'do-care'
            or await post_exists_cached(db, uri) == 'do-care'
        )

        if not care_about_something_here:
            continue

        if await post_exists_cached(db, uri) == 'not-here':
            continue

        can_store_immediately = True

        if await user_exists_cached(db, like['author']) == 'not-here':
            unknown_things_to_queue.append((like['author'], 'actor'))
            can_store_immediately = False

        if await post_exists_cached(db, uri) == 'not-here':
            unknown_things_to_queue.append((uri, 'post'))
            can_store_immediately = False

        if not can_store_immediately:
            unknown_things_to_queue.append((like['uri'], 'like'))
        else:
            served_post = await db.servedpost.find_first(
                where={
                    "when": {"gt": datetime.now() - timedelta(minutes=5)},
                    "post_uri": like["record"].subject.uri,
                    "client_did": like["author"],
                }
            )

            if served_post is not None:
                print(f"Someone liked a post, attirbuted to", served_post.feed_name)

            likes_to_create.append({
                "uri": like["uri"],
                "cid": like["cid"],
                "liker_id": like["author"],
                "post_uri": like["record"].subject.uri,
                "post_cid": like["record"].subject.cid,
                "created_at": parse_datetime(like["record"].created_at),
                "attributed_feed": None
                if served_post is None
                else served_post.feed_name,
            })

    if unknown_things_to_queue:
        # cprint('Unknown things', 'red', force_color=True)
        # print(unknown_things_to_queue)
        await db.unknownthing.create_many(
            [
                {'identifier': i, 'kind': k}
                for i, k in unknown_things_to_queue
            ],
            skip_duplicates=True
        )

    if likes_to_create:
        await db.like.create_many(data=likes_to_create, skip_duplicates=True)

    # TODO: Handle deleted likes lmao

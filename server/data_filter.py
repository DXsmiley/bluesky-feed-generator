from atproto import models

from server.logger import logger
from server.data_stream import OpsByType

from typing import List
from prisma.types import PostCreateWithoutRelationsInput
import prisma.errors

from server.database import Database, care_about_storing_user_data_preemptively
from server.load_known_furries import parse_datetime

from server.util import mentions_fursuit, parse_datetime

from datetime import datetime, timedelta


async def operations_callback(db: Database, ops: OpsByType) -> None:
    # Here we can filter, process, run ML classification, etc.
    # After our feed alg we can save posts into our DB
    # Also, we should process deleted posts to remove them from our DB and keep it in sync

    # for example, let's create our custom feed that will contain all posts that contains fox related text

    posts_to_create: List[PostCreateWithoutRelationsInput] = []
    for created_post in ops["posts"]["created"]:
        author_did = created_post["author"]
        record = created_post["record"]

        inlined_text = record.text.replace("\n", " ")
        images = (
            record.embed.images
            if isinstance(record.embed, models.AppBskyEmbedImages.Main)
            else []
        )
        images_with_alt_text = [i for i in images if i.alt.strip() != ""]
        image_urls = {
            index: f"https://av-cdn.bsky.app/img/feed_thumbnail/plain/{author_did}/{image.image.cid}@jpeg"
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

        if (
            await db.actor.find_first(
                where={
                    "did": author_did,
                    "AND": [care_about_storing_user_data_preemptively],
                }
            )
        ) is not None:
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
        await db.post.create_many(posts_to_create)

    posts_to_delete = [p["uri"] for p in ops["posts"]["deleted"]]
    if posts_to_delete:
        deleted_rows = await db.post.delete_many(where={"uri": {"in": posts_to_delete}})
        if deleted_rows:
            logger.info(f"Deleted from feed: {deleted_rows}")

    for like in ops["likes"]["created"]:
        uri = like["record"]["subject"]["uri"]
        liked_post = await db.post.find_unique({"uri": uri})
        if liked_post is None:
            continue
        like_author = await db.actor.find_first(
            where={
                "did": like["author"],
                "AND": [care_about_storing_user_data_preemptively],
            }
        )
        if like_author is None:
            continue

        girl = (
            like_author.autolabel_fem_vibes and not like_author.autolabel_masc_vibes
        ) or like_author.manual_include_in_vix_feed

        served_post = await db.servedpost.find_first(
            where={
                "when": {"gt": datetime.now() - timedelta(minutes=5)},
                "post_uri": like["record"].subject.uri,
                "client_did": like["author"],
            }
        )

        print(f"{like_author.handle} ({girl}, {served_post is not None}) liked a post")

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

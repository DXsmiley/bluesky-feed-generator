from atproto import models

from server.logger import logger
from server.data_stream import OpsByType

from typing import List
from prisma.types import PostCreateInput

from server.database import Database
from server.load_known_furries import parse_datetime

from server.util import mentions_fursuit, parse_datetime



async def operations_callback(db: Database, ops: OpsByType) -> None:
    # Here we can filter, process, run ML classification, etc.
    # After our feed alg we can save posts into our DB
    # Also, we should process deleted posts to remove them from our DB and keep it in sync

    # for example, let's create our custom feed that will contain all posts that contains fox related text

    posts_to_create: List[PostCreateInput] = []
    for created_post in ops['posts']['created']:
        record = created_post['record']

        images = record.embed.images if isinstance(record.embed, models.AppBskyEmbedImages.Main) else []
        images_with_alt_text = [i for i in images if i.alt.strip() != '']
        inlined_text = record.text.replace('\n', ' ')

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

        # We're not doing anything with replies right now so we'll just ignore them to save cycles
        if reply_parent is not None or reply_root is not None:
            continue

        if (await db.actor.find_unique({'did': created_post['author']})) is not None:
            logger.info(f'New furry post (with images: {len(images)}): {inlined_text}')
            if len(images) > 0:
                print(images)
            post_dict: PostCreateInput = {
                'uri': created_post['uri'],
                'cid': created_post['cid'],
                'reply_parent': reply_parent,
                'reply_root': reply_root,
                'authorId': created_post['author'],
                'text': record.text,
                'mentions_fursuit': mentions_fursuit(record.text),
                'media_count': len(images),
                'media_with_alt_text_count': len(images_with_alt_text),
                'm0': None if len(images) <= 0 else images[0].image.ref,
                'm1': None if len(images) <= 1 else images[1].image.ref,
                'm2': None if len(images) <= 2 else images[2].image.ref,
                'm3': None if len(images) <= 3 else images[3].image.ref,
            }
            posts_to_create.append(post_dict)

    posts_to_delete = [p['uri'] for p in ops['posts']['deleted']]
    if posts_to_delete:
        deleted_rows = await db.post.delete_many(
            where={'uri': {'in': posts_to_delete}}
        )
        if deleted_rows:
            logger.info(f'Deleted from feed: {deleted_rows}')

    if posts_to_create:
        for post in posts_to_create:
            await db.post.create(post)

    for like in ops['likes']['created']:
        uri = like['record']['subject']['uri']
        liked_post = await db.post.find_unique({'uri': uri})
        like_author = await db.actor.find_unique(where={'did': like['author']})
        # TODO: We're gonna be phasing this out at some point
        if liked_post is not None:
            # logger.info(f'Someone liked a furry post!! ({liked_post.like_count})')
            await db.post.update(
                data={'like_count': liked_post.like_count + 1},
                where={'uri': uri}
            )
        if liked_post is not None and like_author is not None:
            print(f'{like_author.handle} ({like_author.gender_label_auto}) liked a post')
            await db.like.create(
                data={
                    'uri': like['uri'],
                    'cid': like['cid'],
                    'liker_id': like['author'],
                    'post_uri': like['record'].subject.uri,
                    'post_cid': like['record'].subject.cid,
                    'created_at': parse_datetime(like['record'].createdAt),
                }
            )

    # TODO: Handle deleted likes lmao

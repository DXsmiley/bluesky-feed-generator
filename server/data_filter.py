from atproto import models

from server.logger import logger
from server.database import Post, Actor
from server.data_stream import OpsByType

from typing import List
from prisma.types import PostCreateInput


def mentions_fursuit(text: str) -> bool:
    text = text.replace('\n', ' ').lower()
    return 'fursuit' in text or 'murrsuit' in text


def operations_callback(ops: OpsByType) -> None:
    # Here we can filter, process, run ML classification, etc.
    # After our feed alg we can save posts into our DB
    # Also, we should process deleted posts to remove them from our DB and keep it in sync

    # for example, let's create our custom feed that will contain all posts that contains fox related text

    posts_to_create: List[PostCreateInput] = []
    for created_post in ops['posts']['created']:
        record = created_post['record']

        num_images = (
            0 if not isinstance(record.embed, models.AppBskyEmbedImages.Main)
            else len(record.embed.images)
        )
        inlined_text = record.text.replace('\n', ' ')

        reply_parent = None
        if record.reply and record.reply.parent.uri:
            reply_parent = record.reply.parent.uri

        reply_root = None
        if record.reply and record.reply.root.uri:
            reply_root = record.reply.root.uri

        if Actor.prisma().find_unique({'did': created_post['author']}) is not None:
            logger.info(f'New furry post (with images: {num_images}): {inlined_text}')
            post_dict: PostCreateInput = {
                'uri': created_post['uri'],
                'cid': created_post['cid'],
                'reply_parent': reply_parent,
                'reply_root': reply_root,
                'authorId': created_post['author'],
                'text': record.text,
                'mentions_fursuit': mentions_fursuit(record.text),
                'media_count': num_images,
            }
            posts_to_create.append(post_dict)

    posts_to_delete = [p['uri'] for p in ops['posts']['deleted']]
    if posts_to_delete:
        Post.prisma().delete_many(
            where={'uri': {'in': posts_to_delete}}
        )
        logger.info(f'Deleted from feed: {len(posts_to_delete)}')

    if posts_to_create:
        for post in posts_to_create:
            Post.prisma().create(post)
        # Post.prisma().create_many(posts_to_create) # create_many not supported by SQLite
        # with db.atomic():
        #     for post_dict in posts_to_create:
        #         Post.create(**post_dict)
        logger.info(f'Added to feed: {len(posts_to_create)}')

    for like in ops['likes']['created']:
        uri = like['record']['subject']['uri']
        liked_post = Post.prisma().find_unique({'uri': uri})
        if liked_post is not None:
            logger.info(f'Someone liked a furry post!! ({liked_post.like_count})')
            Post.prisma().update(
                data={'like_count': liked_post.like_count + 1},
                where={'uri': uri}
            )

    # TODO: Handle deleted likes lmao


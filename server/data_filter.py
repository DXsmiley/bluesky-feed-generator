from atproto import models

from server.logger import logger
from server.database import Post
from server.data_stream import OpsByType

from typing import List
from prisma.types import PostCreateWithoutRelationsInput

import re


def operations_callback(ops: OpsByType) -> None:
    # Here we can filter, process, run ML classification, etc.
    # After our feed alg we can save posts into our DB
    # Also, we should process deleted posts to remove them from our DB and keep it in sync

    # for example, let's create our custom feed that will contain all posts that contains fox related text

    posts_to_create: List[PostCreateWithoutRelationsInput] = []
    for created_post in ops['posts']['created']:
        record = created_post['record']

        # print all texts just as demo that data stream works
        post_with_images = isinstance(record.embed, models.AppBskyEmbedImages.Main)
        inlined_text = record.text.replace('\n', ' ')

        lowertext = inlined_text.lower()

        numlikes = len(ops['likes']['created'])
        if numlikes:
            logger.info(f'<3 x {numlikes}')

        # only fox-related posts, but nothing about fox news
        if re.search(r'\bfox(es)?\b', lowertext) and not re.search(r'\bnews\b', lowertext):
            logger.info(f'New fox post (with images: {post_with_images}): {inlined_text}')

            reply_parent = None
            if record.reply and record.reply.parent.uri:
                reply_parent = record.reply.parent.uri

            reply_root = None
            if record.reply and record.reply.root.uri:
                reply_root = record.reply.root.uri

            post_dict: PostCreateWithoutRelationsInput = {
                'uri': created_post['uri'],
                'cid': created_post['cid'],
                'reply_parent': reply_parent,
                'reply_root': reply_root,
            }
            posts_to_create.append(post_dict)

    posts_to_delete = [p['uri'] for p in ops['posts']['deleted']]
    if posts_to_delete:
        Post.prisma().delete_many(
            where={'uri': {'in': posts_to_delete}}
        )
        # Post.delete().where(Post.uri.in_(posts_to_delete))
        logger.info(f'Deleted from feed: {len(posts_to_delete)}')

    if posts_to_create:
        for post in posts_to_create:
            Post.prisma().create(post)
        # Post.prisma().create_many(posts_to_create) # create_many not supported by SQLite
        # with db.atomic():
        #     for post_dict in posts_to_create:
        #         Post.create(**post_dict)
        logger.info(f'Added to feed: {len(posts_to_create)}')

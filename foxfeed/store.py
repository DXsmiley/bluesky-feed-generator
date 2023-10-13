from foxfeed.database import Database
from atproto.xrpc_client.models.app.bsky.actor.defs import ProfileViewDetailed
from atproto.xrpc_client.models.app.bsky.feed.defs import (
    PostView,
    FeedViewPost,
)
from foxfeed import gender
import random
import prisma.errors
from atproto.xrpc_client.models.app.bsky.embed import images, record, record_with_media
from atproto.xrpc_client.models.app.bsky.feed.get_likes import Like
from atproto.xrpc_client.models.com.atproto.label.defs import Label
from atproto.xrpc_client import models
from foxfeed.util import parse_datetime, ensure_string, mentions_fursuit, is_record_type
from datetime import datetime

from typing import Optional, List, Union, Tuple


async def store_user(
    db: Database,
    user: ProfileViewDetailed,
    *,
    is_muted: bool,
    is_furrylist_verified: bool,
    flag_for_manual_review: bool,
    is_external_to_network: bool,
) -> None:
    gender_vibes = gender.vibecheck(user.description or "")
    print(user)
    await db.actor.upsert(
        where={"did": user.did},
        data={
            "create": {
                "did": user.did,
                "handle": user.handle,
                "description": user.description,
                "displayName": user.display_name,
                "avatar": user.avatar,
                "flagged_for_manual_review": flag_for_manual_review,
                "autolabel_fem_vibes": gender_vibes.fem,
                "autolabel_nb_vibes": gender_vibes.enby,
                "autolabel_masc_vibes": gender_vibes.masc,
                "is_furrylist_verified": is_furrylist_verified,  # TODO
                "is_muted": is_muted,
                "is_external_to_network": is_external_to_network,
                "follower_count": user.followers_count or 0,
                "following_count": user.follows_count or 0,
            },
            "update": {
                "did": user.did,
                "handle": user.handle,
                "description": user.description,
                "displayName": user.display_name,
                "avatar": user.avatar,
                "autolabel_fem_vibes": gender_vibes.fem,
                "autolabel_nb_vibes": gender_vibes.enby,
                "autolabel_masc_vibes": gender_vibes.masc,
                "is_muted": is_muted,
                "is_external_to_network": is_external_to_network,
                "is_furrylist_verified": is_furrylist_verified,
                # 'flagged_for_manual_review': flag_for_manual_review,
                "follower_count": user.followers_count or 0,
                "following_count": user.follows_count or 0,
            },
        },
    )


async def store_like(
    db: Database, post_uri: str, like: Like
) -> Optional[prisma.models.Like]:
    ugh = datetime.utcnow().isoformat()
    blh = random.randint(0, 1 << 32)
    uri = f"fuck://{ugh}-{blh}"
    try:
        return await db.like.create(
            data={
                "uri": uri,  # TODO
                "cid": "",  # TODO
                "post_uri": post_uri,
                "post_cid": "",  # TODO
                "liker_id": like.actor.did,
                "created_at": parse_datetime(like.created_at),
            }
        )
    except prisma.errors.UniqueViolationError:
        pass
    except prisma.errors.ForeignKeyViolationError:
        pass
    return None


async def store_post(db: Database, post: FeedViewPost, *, now: Optional[datetime] = None) -> None:
    await store_post2(
        db,
        post.post,
        None if post.reply is None else post.reply.parent.uri,
        None if post.reply is None else post.reply.root.uri,
        datetime.now() if now is None else now
    )


async def store_post3(db: Database, post: PostView, *, now: Optional[datetime] = None) -> None:
    root, parent = (
        (post.record.reply.root.uri, post.record.reply.parent.uri)
        if (is_record_type(post.record, models.AppBskyFeedPost)
        and post.record.reply is not None)
        else (None, None)
    )
    await store_post2(
        db,
        post,
        parent,
        root,
        datetime.now() if now is None else now,
    )


def labels_to_strings(labels: List[Label]) -> List[str]:
    return [
        ('-' if i.neg else '') + i.val
        for i in sorted(labels, key=lambda l: l.cts)
    ]


def get_media(p: PostView) -> List[images.ViewImage]:
    if isinstance(p.embed, record_with_media.View) and isinstance(p.embed.media, images.View):
        return p.embed.media.images
    if isinstance(p.embed, images.View):
        return p.embed.images
    return []


def get_quoted_skeet(p: PostView) -> Union[Tuple[str, str], Tuple[None, None]]:
    if isinstance(p.embed, record_with_media.View) and isinstance(p.embed.record, record.ViewRecord):
        return (p.embed.record.uri, p.embed.record.cid)
    if isinstance(p.embed, record.ViewRecord):
        return (p.embed.uri, p.embed.cid)
    return (None, None)


async def store_post2(db: Database, p: PostView, reply_parent: Optional[str], reply_root: Optional[str], now: datetime) -> None:
    media = get_media(p)
    media_with_alt_text = sum(i.alt != "" for i in media)
    # if verbose:
    #     print(f'- ({p.uri}, {media_count} images, {p.likeCount or 0} likes) - {p.record["text"]}')
    text = '(p.record was not a models.AppBskyFeedPost)'
    if is_record_type(p.record, models.AppBskyFeedPost):
        text = ensure_string(p.record.text or '')
    labels = labels_to_strings(p.labels or [])
    embed_uri, embed_cid = get_quoted_skeet(p)
    create: prisma.types.PostCreateInput = {
        "uri": p.uri,
        "cid": p.cid,
        # TODO: Fix these
        "reply_parent": reply_parent,
        "reply_root": reply_root,
        "indexed_at": parse_datetime(p.indexed_at),
        "like_count": p.like_count or 0,
        "authorId": p.author.did,
        "mentions_fursuit": mentions_fursuit(text),
        "media_count": len(media),
        "media_with_alt_text_count": media_with_alt_text,
        "text": text,
        "labels": labels,
        "m0": None if len(media) <= 0 else media[0].thumb,
        "m1": None if len(media) <= 1 else media[1].thumb,
        "m2": None if len(media) <= 2 else media[2].thumb,
        "m3": None if len(media) <= 3 else media[3].thumb,
        "embed_uri": embed_uri,
        "embed_cid": embed_cid,
    }
    update: prisma.types.PostUpdateInput = {
        "like_count": p.like_count or 0,
        "media_count": len(media),
        "media_with_alt_text_count": media_with_alt_text,
        "mentions_fursuit": mentions_fursuit(text),
        "text": text,
        "labels": labels,
        "m0": None if len(media) <= 0 else media[0].thumb,
        "m1": None if len(media) <= 1 else media[1].thumb,
        "m2": None if len(media) <= 2 else media[2].thumb,
        "m3": None if len(media) <= 3 else media[3].thumb,
        "last_rescan": now,
        "embed_uri": embed_uri,
        "embed_cid": embed_cid,
    }
    await db.post.upsert(
        where={"uri": p.uri},
        data={
            "create": create,
            "update": update,
        },
    )
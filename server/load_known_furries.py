from datetime import datetime, timezone, timedelta

import server.monkeypatch

from server.database import db

from atproto import Client

from publish_feed import HANDLE, PASSWORD

from typing import Iterable, List, Optional, Dict

from atproto.xrpc_client.models.app.bsky.actor.defs import ProfileView, ProfileViewDetailed
from atproto.xrpc_client.models.app.bsky.feed.defs import FeedViewPost

import re
import json


def is_girl(description: Optional[str]) -> bool:
    if description is None:
        return False
    desc = description.replace('\n', ' ').lower()
    # he/him results in False (to catch cases of he/she/they)
    if re.search(r'\bhe\b', desc):
        return False
    if re.search(r'\bhim\b', desc):
        return False
    # she/her pronouns
    if re.search(r'\bshe\b', desc):
        return True
    if re.search(r'\bher\b', desc):
        return True
    # Emoji
    if '♀️' in desc or '⚢' in desc:
        return True
    # look for cases of "25F" or something similar
    if re.search(r'\b(\d\d)?f\b', desc):
        return True
    # they/them intentionally not considered,
    # but if we've seen nothing by now we bail
    return False


def simplify_profile_view(p: ProfileViewDetailed) -> ProfileView:
    return ProfileView(
        did=p.did,
        handle=p.handle,
        avatar=p.avatar,
        description=p.description,
        displayName=p.displayName,
        indexedAt=p.indexedAt,
        labels=p.labels,
        viewer=p.viewer
    )


def get_followers(client: Client, did: str) -> Iterable[ProfileView]:
    r = client.bsky.graph.get_followers({'actor': did})
    yield from r.followers
    while r.cursor:
        r = client.bsky.graph.get_followers({'actor': did, 'cursor': r.cursor})
        yield from r.followers


def get_follows(client: Client, did: str) -> Iterable[ProfileView]:
    r = client.bsky.graph.get_follows({'actor': did})
    yield from r.follows
    while r.cursor:
        r = client.bsky.graph.get_follows({'actor': did, 'cursor': r.cursor})
        yield from r.follows


def parse_datetime(s: str) -> datetime:
    return datetime.strptime(s.strip('Z'), r'%Y-%m-%dT%H:%M:%S.%f')


def get_posts(client: Client, did: str, after: datetime) -> Iterable[FeedViewPost]:
    r = client.bsky.feed.get_author_feed({'actor': did})
    for i in r.feed:
        if parse_datetime(i.post.record['createdAt']) < after:
            return
        yield i
    while r.cursor:
        r = client.bsky.feed.get_author_feed({'actor': did, 'cursor': r.cursor})
        for i in r.feed:
            if parse_datetime(i.post.record['createdAt']) < after:
                return
            yield i


def load() -> None:
    client = Client()
    client.login(HANDLE, PASSWORD)

    only_posts_after = datetime.now() - timedelta(hours=50)

    # Accounts picked because they're large and the people following them
    # are most likely furries. Doesn't matter 
    known_furries_handles = [
        'puppyfox.bsky.social',
        'furryli.st',
        'braeburned.com',
        '100racs.bsky.social',
        'glitzyfox.bsky.social',
        'itswolven.bsky.social',
        'coolkoinu.bsky.social',
        'gutterbunny.bsky.social',
        'zoeydogy.bsky.social'
    ]

    print('Grabbing known furries...')

    furries: Dict[str, ProfileView] = {}

    for handle in known_furries_handles:
        print('Known furry:', handle)
        profile = simplify_profile_view(client.bsky.actor.get_profile({'actor': handle}))
        furries[profile.did] = profile
        for follower in get_followers(client, profile.did):
            print('Follower:', follower.handle, flush=False)
            furries[follower.did] = follower

    print('Adding furries to database')

    for user in furries.values():
        print(user.handle, flush=False)
        db.actor.upsert(
            where={'did': user.did},
            data={
                'create': {
                    'did': user.did,
                    'handle': user.handle,
                    'description': user.description,
                    'displayName': user.displayName,
                    'in_fox_feed': True,
                    'in_vix_feed': is_girl(user.description),
                },
                'update': {
                    'did': user.did,
                    'handle': user.handle,
                    'description': user.description,
                    'displayName': user.displayName,
                    'in_fox_feed': True,
                    'in_vix_feed': is_girl(user.description),
                }
            }
        )

    print('Grabbing posts for database')

    for user in sorted(furries.values(), key=lambda i: is_girl(i.description), reverse=True):
        print(user.handle, flush=False)
        for post in get_posts(client, user.did, only_posts_after):
            p = post.post
            # this filters out furry reposts from non-furry accounts
            # not an intentional choice but we violate the foreign key otherwise lmao
            if db.actor.find_unique({'did': p.author.did}) is not None:
                db.post.upsert(
                    where={'uri': p.uri},
                    data={
                        'create': {
                            'uri': p.uri,
                            'cid': p.cid,
                            # TODO: Fix these
                            'reply_parent': None if post.reply is None else post.reply.parent.uri,
                            'reply_root': None if post.reply is None else post.reply.root.uri,
                            'indexed_at': parse_datetime(p.record['createdAt']),
                            'like_count': p.likeCount or 0,
                            'authorId': p.author.did,
                        },
                        'update': {
                            'like_count': p.likeCount or 0,
                        }
                    }
                )

    print('Done')

if __name__ == '__main__':
    load()

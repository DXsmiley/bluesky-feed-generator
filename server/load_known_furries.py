from datetime import datetime, timezone, timedelta

import server.monkeypatch

from server.database import db

from atproto import Client, models

from publish_feed import HANDLE, PASSWORD

from typing import Iterable, Optional, Dict, Tuple

from atproto.xrpc_client.models.app.bsky.actor.defs import ProfileView, ProfileViewDetailed
from atproto.xrpc_client.models.app.bsky.feed.defs import FeedViewPost, ReasonRepost
from atproto.xrpc_client.models.app.bsky.graph.defs import ListView, ListItemView
from atproto.xrpc_client.models.app.bsky.embed import images

import unicodedata
import re
import traceback
from termcolor import cprint


import server.algos.fox_feed
from server.data_filter import mentions_fursuit


def is_girl(user: ProfileView) -> bool:
    text = ((user.displayName or '') + ' ' + (user.description or '')).strip()
    if text == '':
        return False
    desc = unicodedata.normalize('NFKC', text).replace('\n', ' ').lower()
    # he/him results in False (to catch cases of he/she/they)
    if re.search(r'\bhe\b', desc):
        return False
    if re.search(r'\bhim\b', desc):
        return False
    # Emoji
    if '♀️' in desc or '⚢' in desc:
        return True
    # look for cases of "25F" or something similar
    if re.search(r'\b\d\df\b', desc):
        return True
    # singular words
    words = [
        'she',
        'her',
        'f',
        'woman',
        'female',
        'girl',
        'transgirl',
        'tgirl',
        'transwoman',
        'puppygirl',
        'doggirl',
    ]
    for w in words:
        if re.search(r'\b' + w + r'\b', desc):
            return True
    # they/them intentionally not considered
    # if we've seen nothing by now we bail
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


def get_mutuals(client: Client, did: str) -> Iterable[ProfileView]:
    following_dids = {i.did for i in get_follows(client, did)}
    for i in get_followers(client, did):
        if i.did in following_dids:
            yield i


def parse_datetime(s: str) -> datetime:
    formats = [
        r'%Y-%m-%dT%H:%M:%S.%fZ',
        r'%Y-%m-%dT%H:%M:%S.%f',
        r'%Y-%m-%dT%H:%M:%SZ',
        r'%Y-%m-%dT%H:%M:%S',
        r'%Y-%m-%dT%H:%M:%S.%f+00:00',
        r'%Y-%m-%dT%H:%M:%S+00:00',
    ]
    for fmt in formats:
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass
    raise ValueError(f'failed to parse datetime string "{s}"')


def get_posts(
    client: Client,
    did: str,
    *,
    after: Optional[datetime]=None,
    include_reposts: bool=False,
    return_data_if_we_have_it_anyway: bool=False
) -> Iterable[FeedViewPost]:
    r = None
    while r is None or r.cursor:
        r = client.bsky.feed.get_author_feed({'actor': did, 'cursor': r and r.cursor})
        for i in r.feed:
            is_repost, indexed_at = (
                (True, i.reason.indexedAt) if isinstance(i.reason, ReasonRepost)
                else (False, i.post.indexedAt)
            )
            if after is not None and parse_datetime(indexed_at) < after:
                r.cursor = None
                if not return_data_if_we_have_it_anyway:
                    return
            if include_reposts or not is_repost:
                yield i


def get_mute_lists(client: Client) -> Iterable[ListView]:
    r = client.bsky.graph.get_list_mutes({})
    yield from r.lists
    while r.cursor:
        r = client.bsky.graph.get_list_mutes({'cursor': r.cursor})
        yield from r.lists


def get_all_mutes(client: Client) -> Iterable[Tuple[ListView, ListItemView]]:
    lists = get_mute_lists(client)
    for lst in lists:
        r = client.bsky.graph.get_list({'list': lst.uri})
        yield from ((lst, i) for i in r.items)
        while r.cursor:
            r = client.bsky.graph.get_list({'list': lst.uri, 'cursor': r.cursor})
            yield from ((lst, i) for i in r.items)


def load() -> None:
    client = Client()
    client.login(HANDLE, PASSWORD)

    only_posts_after = datetime.now() - server.algos.fox_feed.LOOKBACK_HARD_LIMIT

    # Accounts picked because they're large and the people following them are most likely furries
    known_furries = [
        (get_follows, 'puppyfox.bsky.social'),
        (get_follows, 'furryli.st'),
        (get_mutuals, 'brae.gay'),
        (get_mutuals, '100racs.bsky.social'),
        (get_mutuals, 'glitzyfox.bsky.social'),
        (get_mutuals, 'itswolven.bsky.social'),
        (get_mutuals, 'coolkoinu.bsky.social'),
        (get_mutuals, 'gutterbunny.bsky.social'),
        (get_mutuals, 'zoeydogy.bsky.social')
    ]

    known_furries_handles = {i for _, i in known_furries}

    # TODO: Need something better, this is just a rudimentary filter for shit people and dumb gimmick accounts
    print("Grabbing everyone that's on a mute list that I'm subscribed to")

    mutes = get_all_mutes(client)

    # Make a set for fast lookup, also make a cutout for the known furries in case someone adds me to a mutelist
    # without my knowledge or something lmao
    muted_dids = {i.subject.did for _, i in mutes if i.subject.handle not in known_furries_handles}

    # for lst, m in mutes:
    #     print(lst.name, m.subject.handle, m.subject.displayName, ':', (m.subject.description or '').replace('\n', ' ')[:100])

    print('Grabbing known furries...')

    furries: Dict[str, ProfileView] = {}

    for get_associations, handle in known_furries:
        print('Known furry:', handle)
        profile = client.bsky.actor.get_profile({'actor': handle})
        print(f'({profile.followsCount} followers)')
        furries[profile.did] = simplify_profile_view(profile)
        for other in get_associations(client, profile.did):
            furries[other.did] = other

    print(f'Adding {len(furries)} furries to database')

    for user in furries.values():
        muted = user.did in muted_dids
        db.actor.upsert(
            where={'did': user.did},
            data={
                'create': {
                    'did': user.did,
                    'handle': user.handle,
                    'description': user.description,
                    'displayName': user.displayName,
                    'in_fox_feed': True and not muted,
                    'in_vix_feed': is_girl(user) and not muted,
                },
                'update': {
                    'did': user.did,
                    'handle': user.handle,
                    'description': user.description,
                    'displayName': user.displayName,
                    'in_fox_feed': True and not muted,
                    'in_vix_feed': is_girl(user) and not muted,
                }
            }
        )

    print('Grabbing posts for database')

    for user in sorted(furries.values(), key=lambda i: is_girl(i), reverse=True):
        try:
            print('Getting posts for', user.handle)
            for post in get_posts(client, user.did, after=only_posts_after):
                p = post.post
                reply_parent = None if post.reply is None else post.reply.parent.uri
                reply_root = None if post.reply is None else post.reply.root.uri
                # TODO: Probably remove this later!!! But for now we don't care about replies.
                if reply_parent is not None or reply_root is not None:
                    continue
                media_count = (
                    0 if not isinstance(p.embed, images.View)
                    else len(p.embed.images)
                )
                db.post.upsert(
                    where={'uri': p.uri},
                    data={
                        'create': {
                            'uri': p.uri,
                            'cid': p.cid,
                            # TODO: Fix these
                            'reply_parent': reply_parent,
                            'reply_root': reply_root,
                            'indexed_at': parse_datetime(p.record['createdAt']),
                            'like_count': p.likeCount or 0,
                            'authorId': p.author.did,
                            'mentions_fursuit': mentions_fursuit(p.record['text']),
                            'media_count': media_count,
                            'text': p.record['text'],
                        },
                        'update': {
                            'like_count': p.likeCount or 0,
                            'media_count': media_count,
                            'mentions_fursuit': mentions_fursuit(p.record['text']),
                            'text': p.record['text'],
                        }
                    }
                )
        except Exception:
            cprint(f'error while getting posts for user {user.handle}', color='red')
            traceback.print_exc()

    cprint('Done scraping website :)', color='green')

if __name__ == '__main__':
    load()


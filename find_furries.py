import server.monkeypatch

from atproto import Client

from publish_feed import HANDLE, PASSWORD

from typing import Iterable, List

from atproto.xrpc_client.models.app.bsky.actor.defs import ProfileView, ProfileViewDetailed

import re
import json


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


def main() -> None:
    client = Client()
    client.login(HANDLE, PASSWORD)
    
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

    known_furries: List[ProfileView] = [
        simplify_profile_view(client.bsky.actor.get_profile({'actor': handle}))
        for handle in known_furries_handles
    ]

    print('Grabbing their followers...')

    probably_furries = [
        user
        for source in known_furries
        for user in get_followers(client, source.did)
    ]

    deduped = {
        user.did: user
        for user in known_furries + probably_furries
    }

    blob = {
        'furries': [
            {
                'did': did,
                'handle': user.handle,
                'description': user.description,
                'displayName': user.displayName,
            }
            for did, user in deduped.items()
        ]
    }

    with open('known_furries.json', 'w') as f:
        json.dump(blob, f, indent=4)

    print('Done')


if __name__ == '__main__':
    main()


from . import fox_feed

import os
from typing import Dict, Callable, Optional, TypedDict, List

Handler = Callable[[Optional[str], int], fox_feed.HandlerResult]


def environment_variable_name_for(record_name: str) -> str:
    return 'FEED_URI_' + record_name.upper().replace('-', '_')


class AlgorithmDetails(TypedDict):
    record_name: str
    display_name: str
    description: str
    handler: Handler


algo_details: List[AlgorithmDetails] = [
    {
        'record_name': 'fox-feed',
        'display_name': '🦊 Furry',
        'description': 'Algorithmic feed for furry posts! Details at bsky.probablyaweb.site',
        'handler': fox_feed.fox_feed,
    },
    {
        'record_name': 'vix-feed',
        'display_name': '🦊 Vix',
        'description': 'Algorithmic feed for posts from furry women! Details at bsky.probablyaweb.site',
        'handler': fox_feed.vix_feed,
    },
    {
        'record_name': 'fursuit-feed',
        'display_name': '🦊 Fursuits',
        'description': '(in development)',
        'handler': fox_feed.fursuit_feed,
    }
]

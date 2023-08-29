from . import fox_feed
from .feed_names import FeedName

from typing import TypedDict, List


def environment_variable_name_for(record_name: FeedName) -> str:
    return 'FEED_URI_' + record_name.upper().replace('-', '_')


class AlgorithmDetails(TypedDict):
    record_name: FeedName
    display_name: str
    description: str
    handler: fox_feed.HandlerType


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
    },
    {
        'record_name': 'fresh-feed',
        'display_name': '🦊 Fresh',
        'description': 'New and upcomming posts from furry bluesky.',
        'handler': fox_feed.fresh_feed
    },
    {
        'record_name': 'vix-votes',
        'display_name': '🦊 V²',
        'description': 'Top furry posts, as *voted by* furry women! Details at bsky.probablyaweb.site',
        'handler': fox_feed.vix_votes,
    }
]

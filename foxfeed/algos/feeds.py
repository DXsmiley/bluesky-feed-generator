from . import handlers
from . import generators
from .feed_names import FeedName

from typing import TypedDict, List, Optional


def environment_variable_name_for(record_name: FeedName) -> str:
    return "FEED_URI_" + record_name.upper().replace("-", "_")


class AlgorithmDetails(TypedDict):
    record_name: FeedName
    display_name: str
    description: str
    handler: handlers.HandlerType
    generator: Optional[generators.GeneratorType]


algo_details: List[AlgorithmDetails] = [
    {
        "record_name": "fox-feed",
        "display_name": "🦊 Furry",
        "description": "Algorithmic feed for furry posts! Details at bsky.probablyaweb.site",
        "handler": handlers.fox_feed,
        "generator": generators.fox_feed,
    },
    {
        "record_name": "vix-feed",
        "display_name": "🦊 Vix",
        "description": "Algorithmic feed for posts from furry women! Details at bsky.probablyaweb.site",
        "handler": handlers.vix_feed,
        "generator": generators.vix_feed,
    },
    {
        "record_name": "fursuit-feed",
        "display_name": "🦊 Fursuits",
        "description": "(in development)",
        "handler": handlers.fursuit_feed,
        "generator": None,
    },
    {
        "record_name": "fresh-feed",
        "display_name": "🦊 Fresh",
        "description": "New and upcomming posts from furry bluesky! Details at bsky.probablyaweb.site",
        "handler": handlers.fresh_feed,
        "generator": generators.fresh_feed,
    },
    {
        "record_name": "vix-votes",
        "display_name": "🦊 V²",
        "description": "Top furry posts, as *voted by* furry women! Details at bsky.probablyaweb.site",
        "handler": handlers.vix_votes,
        "generator": generators.vix_votes,
    },
    {
        "record_name": "bisexy",
        "display_name": "🔥 Bi",
        "description": "(in development)",
        "handler": handlers.bisexy,
        "generator": None,
    },
    {
        "record_name": "top-feed",
        "display_name": "🦊 Top",
        "description": "(in development)",
        "handler": handlers.top_feed,
        "generator": generators.top_feed,
    },
]

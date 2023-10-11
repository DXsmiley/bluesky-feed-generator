from . import handlers
from . import generators
from .feed_names import FeedName

from typing import TypedDict, List, Optional


class AlgorithmDetails(TypedDict):
    record_name: FeedName
    display_name: str
    description: str
    handler: handlers.HandlerType
    generator: Optional[generators.GeneratorType]
    show_on_main_account: bool
    show_on_personal_account: bool


algo_details: List[AlgorithmDetails] = [
    {
        "record_name": "fox-feed",
        "display_name": "🦊 Furry",
        "description": "Algorithmic furry feed! Details at bsky.probablyaweb.site",
        "handler": handlers.fox_feed,
        "generator": generators.fox_feed,
        "show_on_main_account": True,
        "show_on_personal_account": True,
    },
    {
        "record_name": "vix-feed",
        "display_name": "🦊 Vix",
        "description": "Algorithmic furry feed for posts from women! Details at bsky.probablyaweb.site",
        "handler": handlers.vix_feed,
        "generator": generators.vix_feed,
        "show_on_main_account": True,
        "show_on_personal_account": True,
    },
    {
        "record_name": "fursuit-feed",
        "display_name": "🦊 Fursuits",
        "description": "(in development)",
        "handler": handlers.fursuit_feed,
        "generator": None,
        "show_on_main_account": True,
        "show_on_personal_account": True,
    },
    {
        "record_name": "fresh-feed",
        "display_name": "🦊 Fresh",
        "description": "New and upcomming posts from furry bluesky! Details at bsky.probablyaweb.site",
        "handler": handlers.fresh_feed,
        "generator": generators.fresh_feed,
        "show_on_main_account": True,
        "show_on_personal_account": True,
    },
    {
        "record_name": "vix-votes",
        "display_name": "🦊 V²",
        "description": "Top furry posts, as *voted by* furry women! Details at bsky.probablyaweb.site",
        "handler": handlers.vix_votes,
        "generator": generators.vix_votes,
        "show_on_main_account": True,
        "show_on_personal_account": True,
    },
    {
        "record_name": "bisexy",
        "display_name": "🔥 Bi",
        "description": "(in development)",
        "handler": handlers.bisexy,
        "generator": None,
        "show_on_main_account": False,
        "show_on_personal_account": False,
    },
    {
        "record_name": "top-feed",
        "display_name": "🦊 Top",
        "description": "(in development)",
        "handler": handlers.top_feed,
        "generator": generators.top_feed,
        "show_on_main_account": False,
        "show_on_personal_account": False,
    },
    {
        "record_name": "quotes-feed",
        "display_name": "🦊 Quotes",
        "description": "(in development)",
        "handler": handlers.quotes_feed,
        "generator": None,
        "show_on_main_account": False,
        "show_on_personal_account": False,
    }
]

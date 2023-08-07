from . import fox_feed

import os
from typing import Dict, Callable, Optional

Handler = Callable[[Optional[str], int], fox_feed.HandlerResult]

algos: Dict[str, Handler] = {
    os.environ['FEED_URI_FOX_FEED']: fox_feed.fox_feed,
    os.environ['FEED_URI_VIX_FEED']: fox_feed.vix_feed,
}

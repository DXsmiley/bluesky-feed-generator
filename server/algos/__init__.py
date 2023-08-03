from . import whats_alf

from typing import Dict, Callable, Optional

Handler = Callable[[Optional[str], int], whats_alf.HandlerResult]

algos: Dict[str, Handler] = {
    whats_alf.uri: whats_alf.handler
}

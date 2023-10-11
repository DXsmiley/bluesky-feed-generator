import asyncio
from dataclasses import dataclass

from foxfeed.database import Database
from foxfeed.bsky import AsyncClient


@dataclass
class Res:
    db: Database
    client: AsyncClient
    personal_bsky_client: AsyncClient
    shutdown_event: asyncio.Event

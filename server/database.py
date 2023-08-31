import prisma
from prisma.types import HttpConfig, DatasourceOverride

from typing import Optional

Database = prisma.Prisma
Post = prisma.models.Post
SubscriptionState = prisma.models.SubscriptionState
Actor = prisma.models.Actor
PostScore = prisma.models.PostScore


async def make_database_connection(url: Optional[str] = None, timeout: int = 10, log_queries: bool = False) -> Database:
    db = prisma.Prisma(
        connect_timeout=timeout,
        http=HttpConfig(timeout=timeout),
        datasource=(None if url is None else DatasourceOverride(url=url)),
        log_queries=log_queries
    )
    await db.connect()
    return db


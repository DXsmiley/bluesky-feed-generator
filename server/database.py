import prisma
from prisma.types import HttpConfig

Database = prisma.Prisma
Post = prisma.models.Post
SubscriptionState = prisma.models.SubscriptionState
Actor = prisma.models.Actor
PostScore = prisma.models.PostScore


async def make_database_connection() -> Database:
    db = prisma.Prisma(
        connect_timeout=10,
        http=HttpConfig(timeout=10),
        # log_queries=True
    )
    await db.connect()
    return db

import asyncio
from server.database import make_database_connection
from datetime import datetime, timedelta

async def main():
    now = datetime.utcnow()
    db = await make_database_connection(timeout=120)
    await db.postscore.delete_many(
        where={'created_at': {'lt': now - timedelta(hours=2)}}
    )


asyncio.run(main())

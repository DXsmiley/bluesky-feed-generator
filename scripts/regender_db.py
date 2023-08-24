import asyncio
from server.database import make_database_connection
from server.gender import guess_gender_reductive
from collections import defaultdict
from typing import Dict

async def main():
    db = await make_database_connection()
    users = await db.actor.find_many()
    c: Dict[str, int] = defaultdict(lambda: 0)
    for i in users:
        gender = guess_gender_reductive(i.description or '')
        c[gender] += 1
        await db.actor.update(
            where={'did': i.did},
            data={'gender_label_auto': gender}
        )
    print(c)

asyncio.run(main())

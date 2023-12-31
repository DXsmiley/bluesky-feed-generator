import asyncio
from foxfeed.database import make_database_connection
import gzip
import json

async def main():
    db = await make_database_connection()
    girls = await db.actor.find_many(where={'in_vix_feed': True})
    blob = {'seed': [i.did for i in girls]}
    print(len(girls), 'seed accounts')
    with gzip.open('./seed.json.gzip', 'wb') as f:
        f.write(json.dumps(blob).encode('utf-8'))

asyncio.run(main())

import asyncio
import server.logger
from server import data_stream, config
from server.database import make_database_connection
from server.data_filter import operations_callback


async def main():
    db = await make_database_connection()
    await data_stream.run(db, config.SERVICE_DID, operations_callback, None)


if __name__ == "__main__":
    asyncio.run(main())

import asyncio
from foxfeed.firehose import data_stream
import foxfeed.logger
from foxfeed import config
from foxfeed.database import make_database_connection
from foxfeed.data_filter import operations_callback
import signal
from typing import Any


async def main():
    shutdown_event = asyncio.Event()
    def _sigint_handler(*_: Any) -> None:
        print('Got shutdown signal!')
        shutdown_event.set()
        signal.signal(signal.SIGINT, signal.SIG_DFL)
    signal.signal(signal.SIGINT, _sigint_handler)
    db = await make_database_connection()
    await data_stream.run(db, config.SERVICE_DID, operations_callback, shutdown_event)


if __name__ == "__main__":
    asyncio.run(main())

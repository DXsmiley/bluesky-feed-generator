import foxfeed.monkeypatch

import asyncio
import sys
from foxfeed.web.app import create_and_run_webapp, run_services
from typing import List
from foxfeed import config
from typing import Any
import signal
from foxfeed.bsky import make_bsky_client
from foxfeed.database import make_database_connection
from foxfeed.args import parse_args


async def main(args: List[str]) -> int:

    shutdown_event = asyncio.Event()
    
    def _sigint_handler(*_: Any) -> None:
        if shutdown_event.is_set():
            print("Got second shutdown signal, doing hard exit")
            print(shutdown_event)
            sys.exit(1)
        else:
            print("Got shutdown signal!")
            shutdown_event.set()

    loop = asyncio.get_event_loop()
    loop.add_signal_handler(signal.SIGINT, _sigint_handler)

    services = parse_args(list(args))

    if isinstance(services, int):
        return services

    db = await make_database_connection(config.DB_URL, log_queries=services.log_db_queries)
    client = await make_bsky_client(db)

    if services.webserver:
        await create_and_run_webapp(
            config.PORT,
            db,
            client,
            services,
            shutdown_event
        )
    else:
        await run_services(
            db,
            client,
            shutdown_event,
            services
        )
    
    return 0


if __name__ == "__main__":

    sys.exit(asyncio.run(main(sys.argv[1:])))

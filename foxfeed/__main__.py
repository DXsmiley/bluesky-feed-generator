import foxfeed.monkeypatch  # type: ignore

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
from foxfeed.res import Res


async def main(arg_strings: List[str]) -> int:

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

    args = parse_args(list(arg_strings))

    if isinstance(args, int):
        return args

    db = await make_database_connection(config.DB_URL, log_queries=args.log_db_queries)
    client = await make_bsky_client(db, config.HANDLE, config.PASSWORD)
    personal_bsky_client = await make_bsky_client(db, config.PERSONAL_HANDLE, config.PERSONAL_PASSWORD)

    res = Res(
        db=db,
        client=client,
        personal_bsky_client=personal_bsky_client,
        shutdown_event=shutdown_event
    )

    if args.webserver:
        await create_and_run_webapp(
            config.PORT,
            res,
            args,
        )
    else:
        await run_services(
            res,
            args
        )
    
    return 0


if __name__ == "__main__":

    sys.exit(asyncio.run(main(sys.argv[1:])))

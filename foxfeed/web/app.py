import sys
import asyncio

from foxfeed import config
from foxfeed.firehose import data_stream
import foxfeed.web.interface

from aiohttp import web
import aiojobs.aiohttp
import foxfeed.metrics
import foxfeed.web.routes

from foxfeed.data_filter import operations_callback
from foxfeed.algos.score_task import score_posts_forever

import foxfeed.load_known_furries

import foxfeed.database
from foxfeed.database import Database, make_database_connection

from typing import AsyncIterator, Callable, Coroutine, Any, Optional

import traceback
import termcolor
from dataclasses import dataclass

from atproto import AsyncClient
from foxfeed.bsky import make_bsky_client
import signal


@dataclass
class Services:
    scraper: bool = True
    firehose: bool = True
    scores: bool = True
    # probably shouldn't be here tbh
    log_db_queries: bool = False
    admin_panel: bool = False


def create_and_run_webapp(
    *, port: int, db_url: Optional[str], services: Optional[Services] = None
) -> None:
    asyncio.run(
        _create_and_run_webapp(port, db_url, services or Services(), catch_sigint=True)
    )


async def _create_and_run_webapp(
    port: int, db_url: Optional[str], services: Services, *, catch_sigint: bool
) -> None:
    shutdown_event = asyncio.Event()
    if catch_sigint:

        def _sigint_handler(*_: Any) -> None:
            if shutdown_event.is_set():
                print("Got second shutdown signal, doing hard exit")
                sys.exit(0)
            else:
                print("Got shutdown signal!")
                shutdown_event.set()

        signal.signal(signal.SIGINT, _sigint_handler)
    db = await make_database_connection(db_url, log_queries=services.log_db_queries)
    client = await make_bsky_client(db)
    app = create_web_application(shutdown_event, db, client, services)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    await shutdown_event.wait()
    print("Waiting on shutdown event finished")
    await site.stop()
    print("Did site.stop")
    await runner.cleanup()
    print("Did runner.cleanup")
    await runner.shutdown()
    print("Did runner.shutdown")


def create_web_application(
    shutdown_event: asyncio.Event, db: Database, client: AsyncClient, services: Services
) -> web.Application:
    app = web.Application()
    app.add_routes(foxfeed.web.routes.create_route_table(db, client, admin_panel=services.admin_panel))
    app.cleanup_ctx.append(background_tasks(shutdown_event, db, client, services))
    aiojobs.aiohttp.setup(app)
    return app


def background_tasks(
    shutdown_event: asyncio.Event, db: Database, client: AsyncClient, services: Services
) -> Callable[[web.Application], AsyncIterator[None]]:
    async def catch(name: str, c: Coroutine[Any, Any, None]) -> None:
        try:
            await c
            termcolor.cprint(
                f"--------[  {name} finished  ]--------", "green", force_color=True
            )
        except KeyboardInterrupt:
            pass
        except:
            termcolor.cprint(
                f"--------[ Failure in {name} ]--------", "red", force_color=True
            )
            termcolor.cprint(
                "Critical exception in background task", "red", force_color=True
            )
            termcolor.cprint(
                "-------------------------------------", "red", force_color=True
            )
            traceback.print_exc()
            termcolor.cprint(
                "-------------------------------------", "red", force_color=True
            )

    async def f(_: web.Application) -> AsyncIterator[None]:
        scraper = None
        scores = None
        firehose = None
        if services.scraper:
            scraper = asyncio.create_task(
                catch(
                    "LOADDB",
                    foxfeed.load_known_furries.rescan_furry_accounts_forever(
                        shutdown_event, db, client
                    ),
                )
            )
        if services.scores:
            scores = asyncio.create_task(
                catch("SCORES", score_posts_forever(shutdown_event, db, client))
            )
        if services.firehose:
            firehose = asyncio.create_task(
                catch(
                    "FIREHS",
                    data_stream.run(
                        db, config.SERVICE_DID, operations_callback, shutdown_event
                    ),
                )
            )
        yield
        print("We're on the other side of the yield in foxfeed.app.background_tasks:f")
        print("I wonder what that means?")
        if scraper is not None:
            await scraper
        if scores is not None:
            await scores
        if firehose is not None:
            await firehose
        print("Finished waiting on the background tasks to finish!")

    return f






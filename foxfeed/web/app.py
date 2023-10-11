import asyncio

from foxfeed import config
from foxfeed.firehose import data_stream
import foxfeed.web.interface
from foxfeed.post_schedule import run_schedule

from aiohttp import web
import aiojobs.aiohttp
import foxfeed.metrics
import foxfeed.web.routes

from foxfeed.data_filter import operations_callback
from foxfeed.algos.score_task import score_posts_forever

import foxfeed.load_known_furries

from typing import AsyncIterator, Callable, Coroutine, Any

import traceback
import termcolor

from foxfeed.args import Args
from foxfeed.res import Res


async def create_and_run_webapp(
    port: int,
    res: Res,
    args: Args,
) -> None:
    # db = await make_database_connection(db_url, log_queries=services.log_db_queries)
    # client = await make_bsky_client(db)
    app = create_web_application(res, args)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    print("Starting webserver")
    await site.start()
    print("Webserver started")
    await res.shutdown_event.wait()
    print("Waiting on shutdown event finished")
    await site.stop()
    print("Did site.stop")
    await runner.cleanup()
    print("Did runner.cleanup")
    await runner.shutdown()
    print("Did runner.shutdown")


def create_web_application(
    res: Res, args: Args
) -> web.Application:
    app = web.Application()
    app.add_routes(foxfeed.web.routes.create_route_table(res.db, res.client, admin_panel=args.admin_panel, require_login=not args.dont_require_admin_login))
    app.cleanup_ctx.append(webapp_background_tasks(res, args))
    aiojobs.aiohttp.setup(app)
    return app


def webapp_background_tasks(
    res: Res, args: Args
) -> Callable[[web.Application], AsyncIterator[None]]:
    return lambda _: _run_services(res, args, running_in_webapp=True)


async def run_services(
        res: Res,
        args: Args
) -> None:
    async for _ in _run_services(res, args):
        pass


async def _catch_service(name: str, c: Coroutine[Any, Any, None]) -> None:
    try:
        termcolor.cprint(
            f"--------[  {name} started   ]--------", "green", force_color=True
        )
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


async def _run_services(
        res: Res,
        args: Args,
        *,
        running_in_webapp: bool = False
) -> AsyncIterator[None]:
    scraper = None
    scores = None
    firehose = None
    scheduler = None
    if args.scraper:
        scraper = asyncio.create_task(
            _catch_service(
                "LOADDB",
                foxfeed.load_known_furries.rescan_furry_accounts(
                    res.shutdown_event, res.db, res.client, args.forever,
                ),
            )
        )
    if args.scores:
        scores = asyncio.create_task(
            _catch_service("SCORES", score_posts_forever(res.shutdown_event, res.db, res.client, args.forever))
        )
    if args.firehose:
        firehose = asyncio.create_task(
            _catch_service(
                "FIREHS",
                data_stream.run(
                    res.db, config.SERVICE_DID, operations_callback, res.shutdown_event
                ),
            )
        )
    if args.post_scheduler:
        scheduler = asyncio.create_task(
            _catch_service(
                "POSTER",
                run_schedule(res.db, res.personal_bsky_client, res.shutdown_event, args.forever)
            )
        )
    yield
    if running_in_webapp:
        print("Waiting for service tasks to finish")
    if args.forever:
        await res.shutdown_event.wait()
    if scraper is not None:
        await scraper
    if scores is not None:
        await scores
    if firehose is not None:
        await firehose
    if scheduler is not None:
        await scheduler
    if running_in_webapp:
        print("Service tasks finished")



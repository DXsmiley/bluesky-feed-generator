import sys
import asyncio
import secrets

from server import config
from server.firehose import data_stream
import server.interface

from aiohttp import web
import aiojobs.aiohttp
import server.jwt_verification
import server.metrics

import server.algos
from server.data_filter import operations_callback
from server.algos.score_task import score_posts_forever

import server.load_known_furries

import prisma
import server.database
from server.database import Database, make_database_connection

from typing import AsyncIterator, Callable, Coroutine, Any, Optional, Set, List, Tuple

import traceback
import termcolor
from termcolor import cprint
from datetime import datetime, timedelta
from dataclasses import dataclass

import scripts.find_furry_girls

from atproto import AsyncClient
from server.bsky import make_bsky_client
import signal


algos = {
    **{
        # TODO: make this slightly less hard-coded
        (
            "at://did:plc:j7jc2j2htz5gxuxi2ilhbqka/app.bsky.feed.generator/"
            + i["record_name"]
        ): i["handler"]
        for i in server.algos.algo_details
    },
    **{i["record_name"]: i["handler"] for i in server.algos.algo_details},
}


# def sigint_handler(*_: Any) -> Never:
#     print('Stopping data stream...')
#     stream_stop_event.set()
#     sys.exit(0)


# signal.signal(signal.SIGINT, sigint_handler)


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
    asyncio.run(_create_and_run_webapp(port, db_url, services or Services(), catch_sigint=True))


async def _create_and_run_webapp(
    port: int, db_url: Optional[str], services: Services, *, catch_sigint: bool
) -> None:
    shutdown_event = asyncio.Event()
    if catch_sigint:
        def _sigint_handler(*_: Any) -> None:
            if shutdown_event.is_set():
                print('Got second shutdown signal, doing hard exit')
                sys.exit(0)
            else:
                print('Got shutdown signal!')
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
    print('Waiting on shutdown event finished')
    await site.stop()
    print('Did site.stop')
    await runner.cleanup()
    print('Did runner.cleanup')
    await runner.shutdown()
    print('Did runner.shutdown')


def create_web_application(shutdown_event: asyncio.Event, db: Database, client: AsyncClient, services: Services) -> web.Application:
    app = web.Application()
    app.add_routes(create_route_table(db, client, admin_panel=services.admin_panel))
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
                    server.load_known_furries.rescan_furry_accounts_forever(shutdown_event, db, client),
                )
            )
        if services.scores:
            scores = asyncio.create_task(catch("SCORES", score_posts_forever(shutdown_event, db, client)))
        if services.firehose:
            firehose = asyncio.create_task(
                catch(
                    "FIREHS",
                    data_stream.run(db, config.SERVICE_DID, operations_callback, shutdown_event),
                )
            )
        yield
        print("We're on the other side of the yield in server.app.background_tasks:f")
        print('I wonder what that means?')
        if scraper is not None:
            await scraper
        if scores is not None:
            await scores
        if firehose is not None:
            await firehose
        print('Finished waiting on the background tasks to finish!')

    return f


def create_route_table(db: Database, client: AsyncClient, *, admin_panel: bool = False):
    admin_token = secrets.token_urlsafe()

    routes = web.RouteTableDef()

    routes.static("/static", "./static")

    @routes.get("/")
    async def index(  # pyright: ignore[reportUnusedFunction]
        request: web.Request,
    ) -> web.StreamResponse:
        return web.FileResponse("./static/index.html")
    
    @routes.get("/favicon.ico")
    async def favicon(  # pyright: ignore[reportUnusedFunction]
        request: web.Request,
    ) -> web.StreamResponse:
        return web.FileResponse("./static/logo.png")

    @routes.get("/stats")
    async def stats(  # pyright: ignore[reportUnusedFunction]
        request: web.Request,
    ) -> web.Response:
        now = datetime.now()
        metrics = await server.metrics.feed_metrics_for_time_range(
            db,
            None,
            now - server.metrics.METRICS_MAXIMUM_LOOKBACK,
            now,
            timedelta(hours=1),
        )
        page = server.interface.stats_page(
            [
                ("feeds", len(server.algos.algo_details)),
                ("users", await db.actor.count()),
                (
                    "in-fox-feed",
                    await db.actor.count(where=server.database.user_is_in_fox_feed),
                ),
                (
                    "in-vix-feed",
                    await db.actor.count(where=server.database.user_is_in_vix_feed),
                ),
                (
                    "storing-data-for",
                    await db.actor.count(
                        where=server.database.care_about_storing_user_data_preemptively
                    ),
                ),
                ("posts", await db.post.count()),
                ("likes", await db.like.count()),
                ("postscores", await db.postscore.count()),
                ("servedblock", await db.servedblock.count()),
                ("servedpost", await db.servedpost.count()),
            ],
            metrics
        )
        return web.Response(text=str(page), content_type="text/html")

    @routes.get("/user/{handle}")
    async def user_deets(  # pyright: ignore[reportUnusedFunction]
        request: web.Request,
    ) -> web.Response:
        handle = request.match_info["handle"]
        # if not isinstance(handle, str):
        #     return web.HTTPBadRequest(text='requires parameter "handle"')
        user = await db.actor.find_first(where={"handle": handle})
        if user is None:
            return web.HTTPNotFound(text="user not found")
        posts = await db.post.find_many(
            where={"authorId": user.did}, order={"indexed_at": "desc"}
        )
        page = server.interface.user_page(is_admin(request), user, posts)
        return web.Response(text=str(page), content_type="text/html")

    @routes.get("/.well-known/did.json")
    async def did_json(  # pyright: ignore[reportUnusedFunction]
        request: web.Request,
    ) -> web.Response:
        if not config.SERVICE_DID.endswith(config.HOSTNAME):
            return web.HTTPNotFound()

        return web.json_response(
            {
                "@context": ["https://www.w3.org/ns/did/v1"],
                "id": config.SERVICE_DID,
                "service": [
                    {
                        "id": "#bsky_fg",
                        "type": "BskyFeedGenerator",
                        "serviceEndpoint": f"https://{config.HOSTNAME}",
                    }
                ],
            }
        )

    @routes.get("/xrpc/app.bsky.feed.describeFeedGenerator")
    async def describe_feed_generator(  # pyright: ignore[reportUnusedFunction]
        request: web.Request,
    ) -> web.Response:
        feeds = [{"uri": uri} for uri in algos.keys()]
        response = {
            "encoding": "application/json",
            "body": {"did": config.SERVICE_DID, "feeds": feeds},
        }
        return web.json_response(response)

    @routes.get("/xrpc/app.bsky.feed.getFeedSkeleton")
    async def get_feed_skeleton(  # pyright: ignore[reportUnusedFunction]
        request: web.Request,
    ) -> web.Response:
        feed = request.query.get("feed", default="")
        algo = algos.get(feed)
        if not algo:
            return web.HTTPBadRequest(text="Unsupported algorithm")
        
        feed_record_name = feed.split('/')[-1]

        cprint(f"Getting feed {feed_record_name}", "magenta", force_color=True)

        s = datetime.now()

        try:
            cursor = request.query.get("cursor", default=None)
            limit = int(request.query.get("limit", default=20))
            body = await algo(db, cursor, limit)
        except ValueError:
            return web.HTTPBadRequest(text="Malformed Cursor")

        d = datetime.now() - s

        cprint(f"Done in {int(d.total_seconds())}", "magenta", force_color=True)

        await aiojobs.aiohttp.spawn(
            request,
            store_served_posts(
                request.headers.get("Authorization"), s, feed_record_name, cursor, limit, body
            ),
        )

        return web.json_response(body)

    async def store_served_posts(
        auth: Optional[str],
        now: datetime,
        feed_name: str,
        cursor: Optional[str],
        limit: int,
        served: server.algos.fox_feed.HandlerResult,
    ) -> None:
        did = await server.jwt_verification.verify_jwt(auth)
        print("store_served_posts", feed_name, did)
        if did is not None:
            await db.servedblock.create(
                data={
                    "when": now,
                    "cursor": cursor,
                    "limit": limit,
                    "served": len(served["feed"]),
                    "feed_name": feed_name,
                    "client_did": did,
                }
            )
            await db.servedpost.create_many(
                data=[
                    {
                        "when": now,
                        "post_uri": i["post"],
                        "client_did": did,
                        "feed_name": feed_name,
                    }
                    for i in served["feed"]
                ]
            )

    @routes.get("/feed")
    async def get_feeds(  # pyright: ignore[reportUnusedFunction]
        request: web.Request,
    ) -> web.Response:
        page = server.interface.feeds_page(
            [i["record_name"] for i in server.algos.algo_details]
        )
        return web.Response(text=str(page), content_type="text/html")

    @routes.get("/feed/{feed}")
    async def get_feed(  # pyright: ignore[reportUnusedFunction]
        request: web.Request,
    ) -> web.Response:
        feed_name = request.match_info.get("feed", "")
        algo = algos.get(feed_name)
        if algo is None:
            return web.HTTPNotFound(text="Feed not found")

        posts = (await algo(db, None, 50))["feed"]
        full_posts = [
            await db.post.find_unique_or_raise(
                {"uri": i["post"]}, include={"author": True}
            )
            for i in posts
        ]

        page = server.interface.feed_page(is_admin(request), feed_name, full_posts)

        return web.Response(text=str(page), content_type="text/html")

    @routes.get("/feed/{feed}/stats")
    async def get_feed_stats(  # pyright: ignore[reportUnusedFunction]
        request: web.Request,
    ) -> web.Response:
        feed_name = request.match_info.get("feed", "")
        algo = algos.get(feed_name)
        if algo is None:
            return web.HTTPNotFound(text="Feed not found")

        now = datetime.now()

        metrics = await server.metrics.feed_metrics_for_time_range(
            db, feed_name, now - server.metrics.METRICS_MAXIMUM_LOOKBACK, now, timedelta(hours=1)
        )

        page = server.interface.feed_metrics_page(metrics)
        return web.Response(text=str(page), content_type="text/html")

    async def quickflag_candidates_from_feed(
        feed_name: server.algos.FeedName,
    ) -> Set[str]:
        max_version = await db.postscore.find_first_or_raise(
            where={"feed_name": feed_name}, order={"version": "desc"}
        )
        postscores = await db.postscore.find_many(
            where={"feed_name": feed_name, "version": max_version.version}
        )
        posts = await db.post.find_many(
            where={"uri": {"in": [i.uri for i in postscores]}}
        )
        return set(i.authorId for i in posts)

    @routes.get("/quickflag")
    async def quickflag(  # pyright: ignore[reportUnusedFunction]
        request: web.Request,
    ) -> web.Response:
        # pick from feeds that are actually able to contain non-girls
        dids = await quickflag_candidates_from_feed("vix-votes")
        users = await db.actor.find_many(
            take=10,
            order={"flagged_for_manual_review": "desc"},
            where={
                "OR": [
                    {
                        # People who have been marked for reivew, obviously
                        "flagged_for_manual_review": True,
                    },
                    {
                        # People who are hitting the V^2 algo who we might want to include in the Vix Feed
                        # but we can't really discern any information about
                        "did": {"in": list(dids)},
                        "autolabel_fem_vibes": False,
                        "autolabel_masc_vibes": False,
                        "autolabel_nb_vibes": False,
                        "manual_include_in_fox_feed": None,
                        "manual_include_in_vix_feed": None,
                    },
                ]
            },
            include={
                "posts": {
                    "take": 4,
                    "order_by": {"indexed_at": "desc"},
                }
            },
        )
        page = server.interface.quickflag_page(is_admin(request), users)
        return web.Response(text=str(page), content_type="text/html")
    
    @routes.get('/experiment/{name}')
    async def experiment_results(  # pyright: ignore[reportUnusedFunction]
        request: web.Request,
    ):
        experiment = request.match_info.get("name", "")
        media: List[Tuple[float, str, Optional[str]]] = []
        highest_version = await db.experimentresult.find_first(
            order={'experiment_version': 'desc'},
            where={
                'experiment_name': experiment,
                'did_error': False,
            }
        )
        if highest_version is not None:
            sample = await db.experimentresult.find_many(
                take=50,
                where={
                    'experiment_version': highest_version.experiment_version,
                    'experiment_name': experiment,
                    'did_error': False,
                },
                include={'post': True}
            )
            sample.sort(key=lambda x: x.result_score, reverse=True)
            media = [
                (
                    i.result_score,
                    i.result_comment,
                    [i.post.m0, i.post.m1, i.post.m2, i.post.m3][i.media_index]
                )
                for i in sample
                if i.post is not None
            ]
        page = server.interface.media_experiment_page(experiment, media)
        return web.Response(text=str(page), content_type="text/html")

    @routes.get("/admin/login")
    async def login_get(  # pyright: ignore[reportUnusedFunction]
        request: web.Request,
    ) -> web.Response:
        page = server.interface.admin_login_page()
        return web.Response(text=str(page), content_type="text/html")

    @routes.post("/admin/login")
    async def login_post(  # pyright: ignore[reportUnusedFunction]
        request: web.Request,
    ) -> web.Response:
        if not admin_panel or not config.ADMIN_PANEL_PASSWORD:
            return web.HTTPForbidden(text="admin tools currently disabled")
        data = await request.post()
        password = data.get("password")
        assert isinstance(password, str)
        if password == config.ADMIN_PANEL_PASSWORD:
            response = web.HTTPSeeOther("/admin/done-login")
            response.set_cookie("x-foxfeed-admin-login", admin_token)
            return response
        else:
            return web.HTTPForbidden(text="Incorrect password")

    def is_admin(request: web.Request) -> bool:
        r = admin_panel and (
            request.cookies.get("x-foxfeed-admin-login", "") == admin_token
        )
        return r

    def require_admin_login(request: web.Request):
        if not is_admin(request):
            raise web.HTTPForbidden(text="admin tools currently disabled")

    @routes.get("/admin/done-login")
    async def done_login(  # pyright: ignore[reportUnusedFunction]
        request: web.Request,
    ) -> web.Response:
        require_admin_login(request)
        page = server.interface.admin_done_login_page()
        return web.Response(text=str(page), content_type="text/html")

    @routes.post("/admin/mark")
    async def mark_user(  # pyright: ignore[reportUnusedFunction]
        request: web.Request,
    ) -> web.Response:
        require_admin_login(request)
        blob = await request.json()
        did = blob["did"]
        assert isinstance(did, str)
        action: prisma.types.ActorUpdateInput = {"flagged_for_manual_review": False}
        if "include_in_fox_feed" in blob:
            action["manual_include_in_fox_feed"] = blob["include_in_fox_feed"]
        if "include_in_vix_feed" in blob:
            action["manual_include_in_vix_feed"] = blob["include_in_vix_feed"]
        updated = await db.actor.update(
            where={"did": did},
            data=action,
        )
        if updated is None:
            return web.HTTPNotFound(text="user not found")
        return web.HTTPOk(
            text=f"{updated.handle} assigned to fox:{updated.manual_include_in_fox_feed}, vix:{updated.manual_include_in_vix_feed}"
        )

    @routes.post("/admin/scan_likes")
    async def scan_likes(  # pyright: ignore[reportUnusedFunction]
        request: web.Request,
    ) -> web.Response:
        require_admin_login(request)
        blob = await request.json()
        uri = blob["uri"]
        assert isinstance(uri, str)
        added_users, added_likes = await scripts.find_furry_girls.from_likes_of_post(
            db, client, uri
        )
        return web.HTTPOk(
            text=f"Found {added_users} new candidate furries and {added_likes} new likes"
        )

    return routes

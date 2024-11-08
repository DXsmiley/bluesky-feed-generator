# pyright: reportUnusedFunction=false

import asyncio
import secrets
from datetime import datetime, timedelta
from aiohttp import web
import foxfeed.metrics
import foxfeed.web.interface
import foxfeed.algos.feeds
import foxfeed.database
import foxfeed.web.jwt_verification
from foxfeed.database import Database, Post
from foxfeed.bsky import AsyncClient
from foxfeed.web.ratelimit import Ratelimit
from foxfeed import config
from foxfeed.post_schedule import send_post_and_update_db
import foxfeed.algos.generators
from termcolor import cprint
import aiojobs.aiohttp
import prisma
import scripts.find_furry_girls
import gc
from foxfeed import image

from typing import Callable, Coroutine, Any, Optional, Set, List, Tuple, Literal, Union


def data_to_thumbnail(b: bytes) -> bytes:
    img = image.from_bytes(b)
    img = image.scale_down(img, 96)
    return image.to_bytes(img, 'PNG')


def algos_for(did: str, check: Literal['show_on_personal_account', 'show_on_main_account']):
    return {
        f"at://{did}/app.bsky.feed.generator/{i['record_name']}": i['handler']
        for i in foxfeed.algos.feeds.algo_details
        if i[check]
    }


algos = {
    # TODO: don't hard code these DIDs
    **algos_for('did:plc:j7jc2j2htz5gxuxi2ilhbqka', 'show_on_personal_account'),
    **algos_for('did:plc:pflgk5b7l2sbvhusjdzssnla', 'show_on_main_account'),
}


algos_by_short_name = {
    i["record_name"]: i["handler"] for i in foxfeed.algos.feeds.algo_details
}


def create_route_table(
        db: Database,
        client: AsyncClient,
        personal_client: AsyncClient,
        *,
        admin_panel: bool = False,
        require_login: bool = True
) -> web.RouteTableDef:

    if not require_login:
        warning = 'ADMIN LOGIN NOT REQUIRED'
        lines = [
            '*' * (len(warning) + 6),
            '*  ' + (' ' * len(warning)) + '  *',
            '*  ' + warning + '  *',
            '*  ' + (' ' * len(warning)) + '  *',
            '*' * (len(warning) + 6),
        ]
        cprint('\n'.join(lines), 'red', force_color=True)

    auth_ratelimit = Ratelimit(timedelta(seconds=5), limit=10)

    async def is_admin(request: web.Request) -> bool:
        if not require_login:
            return True
        if not admin_panel:
            return False
        token = request.cookies.get("x-foxfeed-admin-login", "")
        if not token:
            return False
        # This is the secret sauce that we don't want to give away, RATELIMIT THIS CHECK
        auth_ratelimit.check_raising()
        return token == admin_token

    def require_admin_login(
        handler: Callable[[web.Request], Coroutine[Any, Any, web.Response]]
    ) -> Callable[[web.Request], Coroutine[Any, Any, web.Response]]:
        async def require_admin_login_wrapped(request: web.Request) -> web.Response:
            if not await is_admin(request):
                if request.method == "GET":
                    raise web.HTTPSeeOther("/admin/login")
                raise web.HTTPForbidden(text="admin tools currently disabled")
            return await handler(request)

        return require_admin_login_wrapped

    admin_token = secrets.token_urlsafe()

    routes = web.RouteTableDef()

    routes.static("/static", "./static")

    @routes.get("/")
    async def index(request: web.Request) -> web.StreamResponse:
        return web.FileResponse("./static/index.html")

    @routes.get("/favicon.ico")
    async def favicon(request: web.Request) -> web.StreamResponse:
        return web.FileResponse("./static/logo.png")

    async def query_stats(now: datetime) -> List[Tuple[str, int]]:
        async def f(s: str, c: Union[int, Coroutine[Any, Any, int]]) -> Tuple[str, int]:
            return (s, c if isinstance(c, int) else await c)
        return await asyncio.gather(
            f("feeds", len(foxfeed.algos.feeds.algo_details)),
            f("users", db.actor.count()),
            f(
                "> in-fox-feed",
                db.actor.count(where=foxfeed.database.user_is_in_fox_feed),
            ),
            f(
                "> in-vix-feed",
                db.actor.count(where=foxfeed.database.user_is_in_vix_feed),
            ),
            f(
                "> storing-data-for",
                db.actor.count(
                    where=foxfeed.database.care_about_storing_user_data_preemptively
                ),
            ),
            f("posts", db.post.count()),
            f(
                "> posts-recent",
                db.post.count(
                    where={"indexed_at": {"gt": now - timedelta(hours=96)}}
                ),
            ),
            f("likes", db.like.count()),
            f(
                "> likes-recent",
                db.like.count(
                    where={"created_at": {"gt": now - timedelta(hours=96)}}
                ),
            ),
            f("postscores", db.postscore.count()),
            f("servedblock", db.servedblock.count()),
            f("servedpost", db.servedpost.count()),
            f("unknownthings", db.unknownthing.count()),
            f("> users", db.unknownthing.count(where={'kind': 'actor'})),
            f("> posts", db.unknownthing.count(where={'kind': 'post'})),
            f("> likes", db.unknownthing.count(where={'kind': 'like'})),
        )

    @routes.get("/stats")
    async def stats(request: web.Request) -> web.Response:
        now = datetime.now()
        metrics_c = foxfeed.metrics.feed_metrics_for_time_range(
            db,
            None,
            now - foxfeed.metrics.METRICS_MAXIMUM_LOOKBACK,
            now,
            timedelta(hours=1),
        )
        qstats_c = query_stats(now)
        qstats, metrics = await asyncio.gather(qstats_c, metrics_c)
        page = foxfeed.web.interface.stats_page(qstats, metrics)
        return web.Response(text=str(page), content_type="text/html")

    @routes.get("/user/{handle}")
    async def user_deets(request: web.Request) -> web.Response:
        handle = request.match_info["handle"]
        # if not isinstance(handle, str):
        #     return web.HTTPBadRequest(text='requires parameter "handle"')
        user = await db.actor.find_first(where={"handle": handle})
        if user is None:
            return web.HTTPNotFound(text="user not found")
        posts = await db.post.find_many(
            where={"authorId": user.did, "reply_root": None},
            order={"indexed_at": "desc"},
        )
        page = foxfeed.web.interface.user_page(await is_admin(request), user, posts)
        return web.Response(text=str(page), content_type="text/html")

    @routes.get("/.well-known/did.json")
    async def did_json(request: web.Request) -> web.Response:
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
    async def describe_feed_generator(request: web.Request) -> web.Response:
        feeds = [{"uri": uri} for uri in algos.keys()]
        response = {
            "encoding": "application/json",
            "body": {"did": config.SERVICE_DID, "feeds": feeds},
        }
        return web.json_response(response)

    @routes.get("/xrpc/app.bsky.feed.getFeedSkeleton")
    async def get_feed_skeleton(request: web.Request) -> web.Response:
        feed = request.query.get("feed", default="")
        feed_record_name = feed.split("/")[-1]
        algo = algos.get(feed)
        if not algo:
            return web.HTTPBadRequest(text="Unsupported algorithm")

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
                request.headers.get("Authorization"),
                s,
                feed_record_name,
                cursor,
                limit,
                body,
            ),
        )

        return web.json_response(body)

    async def store_served_posts(
        auth: Optional[str],
        now: datetime,
        feed_name: str,
        cursor: Optional[str],
        limit: int,
        served: foxfeed.algos.feeds.handlers.HandlerResult,
    ) -> None:
        did = await foxfeed.web.jwt_verification.verify_jwt(auth)
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
    async def get_feeds(request: web.Request) -> web.Response:
        page = foxfeed.web.interface.feeds_page(
            [i["record_name"] for i in foxfeed.algos.feeds.algo_details]
        )
        return web.Response(text=str(page), content_type="text/html")

    @routes.get("/feed/{feed}")
    async def get_feed(request: web.Request) -> web.Response:
        feed_name = request.match_info.get("feed", "")
        cursor = request.rel_url.query.get('cursor', None)
        algo = algos_by_short_name.get(feed_name)
        if algo is None:
            return web.HTTPNotFound(text="Feed not found")

        result = await algo(db, cursor, 50)
        posts = result["feed"]
        full_posts = [
            await db.post.find_first(where={"uri": i["post"]}, include={"author": True})
            for i in posts
        ]

        with_quotes: List[Tuple[Optional[Post], Optional[Post]]] = [
            (i, None if i.embed_uri is None else await db.post.find_first(where={"uri": i.embed_uri}, include={"author": True}))
            for i in full_posts
            if i is not None
        ]

        page = foxfeed.web.interface.feed_page(
            await is_admin(request), feed_name, with_quotes, result["cursor"]
        )

        return web.Response(text=str(page), content_type="text/html")
    
    # Kinda doesn't need admin but this is gonna be *slow* so uh yeah
    @routes.get("/feed/{feed}/timetravel")
    @require_admin_login
    async def get_feed_timetravel(request: web.Request) -> web.Response:
        feed_name = request.match_info.get("feed", "")
        algo = [i for i in foxfeed.algos.feeds.algo_details if i['record_name'] == feed_name]
        if not algo:
            return web.HTTPNotFound(text="Feed not found")
        
        algo = algo[0]
        
        now = datetime.now()

        cols: List[List[Optional[foxfeed.database.Post]]] = []
        
        for hours_ago in [72, 60, 48, 36, 24, 12, 0]:
            dt = now - timedelta(hours=hours_ago)
            if algo['generator'] is not None:
                # Low key bad design but whatever
                rd = foxfeed.algos.generators.RunDetails(
                    run_starttime=dt,
                    run_version=0,
                )
                posts = (await algo['generator'](db, rd))[:20]
                full_posts = [
                    await db.post.find_first(
                        where={"uri": i}, include={"author": True}
                    )
                    for i in posts
                ]
                cols.append(full_posts)

        page = foxfeed.web.interface.feed_timetravel_page(
            cols
        )

        return web.Response(text=str(page), content_type="text/html")


    @routes.get("/pinned_posts")
    async def pinned_posts(request: web.Request) -> web.Response:
        posts = await db.post.find_many(
            order={"indexed_at": "desc"},
            where={"is_pinned": True},
            include={"author": True},
        )
        page = foxfeed.web.interface.post_list_page(
            await is_admin(request), "Pinned Posts", posts
        )
        return web.Response(text=str(page), content_type="text/html")

    @routes.get("/feed/{feed}/stats")
    async def get_feed_stats(request: web.Request) -> web.Response:
        feed_name = request.match_info.get("feed", "")
        algo = algos_by_short_name.get(feed_name)
        if algo is None:
            return web.HTTPNotFound(text="Feed not found")

        now = datetime.now()

        metrics = await foxfeed.metrics.feed_metrics_for_time_range(
            db,
            feed_name,
            now - foxfeed.metrics.METRICS_MAXIMUM_LOOKBACK,
            now,
            timedelta(hours=1),
        )

        page = foxfeed.web.interface.feed_metrics_page(metrics)
        return web.Response(text=str(page), content_type="text/html")

    async def quickflag_candidates_from_feed(
        feed_name: foxfeed.algos.feeds.FeedName,
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
    @require_admin_login
    async def quickflag(request: web.Request) -> web.Response:
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
        page = foxfeed.web.interface.quickflag_page(await is_admin(request), users)
        return web.Response(text=str(page), content_type="text/html")

    @routes.get("/experiment/{name}")
    @require_admin_login
    async def experiment_results(request: web.Request):
        experiment = request.match_info.get("name", "")
        media: List[Tuple[float, str, Optional[str]]] = []
        highest_version = await db.experimentresult.find_first(
            order={"experiment_version": "desc"},
            where={
                "experiment_name": experiment,
                "did_error": False,
            },
        )
        if highest_version is not None:
            sample = await db.experimentresult.find_many(
                take=50,
                where={
                    "experiment_version": highest_version.experiment_version,
                    "experiment_name": experiment,
                    "did_error": False,
                },
                include={"post": True},
            )
            sample.sort(key=lambda x: x.result_score, reverse=True)
            media = [
                (
                    i.result_score,
                    i.result_comment,
                    [i.post.m0, i.post.m1, i.post.m2, i.post.m3][i.media_index],
                )
                for i in sample
                if i.post is not None
            ]
        page = foxfeed.web.interface.media_experiment_page(experiment, media)
        return web.Response(text=str(page), content_type="text/html")

    @routes.get("/admin/login")
    async def login_get(request: web.Request) -> web.Response:
        if not admin_panel or not config.ADMIN_PANEL_PASSWORD:
            page = foxfeed.web.interface.admin_login_page_disabled
        else:
            page = foxfeed.web.interface.admin_login_page
        return web.Response(text=str(page), content_type="text/html")

    @routes.post("/admin/login")
    async def login_post(request: web.Request) -> web.Response:
        if not admin_panel or not config.ADMIN_PANEL_PASSWORD:
            return web.HTTPForbidden(text="admin tools currently disabled")
        data = await request.post()
        password = data.get("password")
        assert isinstance(password, str)
        auth_ratelimit.check_raising()
        if password == config.ADMIN_PANEL_PASSWORD:
            response = web.HTTPSeeOther("/admin/done-login")
            response.set_cookie("x-foxfeed-admin-login", admin_token)
            return response
        else:
            return web.HTTPForbidden(text="Incorrect password")

    @routes.get("/admin/done-login")
    @require_admin_login
    async def done_login(request: web.Request) -> web.Response:
        page = foxfeed.web.interface.admin_done_login_page()
        return web.Response(text=str(page), content_type="text/html")

    @routes.post("/admin/mark")
    @require_admin_login
    async def mark_user(request: web.Request) -> web.Response:
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
    @require_admin_login
    async def scan_likes(request: web.Request) -> web.Response:
        blob = await request.json()
        uri = blob["uri"]
        assert isinstance(uri, str)
        added_users, added_likes = await scripts.find_furry_girls.from_likes_of_post(
            db, client, uri
        )
        return web.HTTPOk(
            text=f"Found {added_users} new candidate furries and {added_likes} new likes"
        )

    @routes.post("/admin/pin_post")
    @require_admin_login
    async def pin_post(request: web.Request) -> web.Response:
        blob = await request.json()
        uri = blob["uri"]
        pin = blob["pin"]
        assert isinstance(uri, str)
        assert isinstance(pin, bool)
        await db.post.update(where={"uri": uri}, data={"is_pinned": pin})
        return web.HTTPOk(text=("pinned post" if pin else "unpinned post"))
    
    @routes.get("/schedule")
    @require_admin_login
    async def schedule(request: web.Request) -> web.Response:
        with_thumbnails: prisma.types.ScheduledPostInclude = {'media': {'include': {'datablobs': {'where': {'label': 'thumbnail'}}}}}
        failed = await db.scheduledpost.find_many(
            order={'id': 'desc'},
            where={'status': {'in': ['failed', 'attempting']}},
            include=with_thumbnails,
        )
        scheduled = await db.scheduledpost.find_many(
            order={'id': 'desc'},
            where={'status': 'scheduled'},
            include=with_thumbnails,
        )
        cancelled = await db.scheduledpost.find_many(
            order={'id': 'desc'},
            where={'status': 'cancelled'},
            include=with_thumbnails,
        )
        page = foxfeed.web.interface.scheduled_posts_page(failed + scheduled + cancelled)
        return web.Response(text=str(page), content_type="text/html")
    
    @routes.post("/schedule")
    @require_admin_login
    async def schedule_post(request: web.Request) -> web.Response:
        data = await request.post()
        print('Scheduling post:')
        print(data)
        label = data.get('maturity', 'none')
        assert isinstance(label, str)
        text = data.get('text', '')
        assert isinstance(text, str)
        alt_text = data.get('alt-text', '')
        assert isinstance(alt_text, str)
        image = data.get('image', None)
        image_content = None
        if isinstance(image, web.FileField):
            image_content = image.file.read()
        # Don't queue empty posts lol
        if text or image_content:
            scheduledpost = await db.scheduledpost.create(
                data={
                    'text': text,
                    'label': (None if label == 'none' else label),
                }
            )
            if image_content:
                scheduledmedia = await db.scheduledmedia.create(
                    data = {
                        'scheduled_post_id': scheduledpost.id,
                        'alt_text': alt_text,
                    }
                )
                await db.mediablob.create_many(
                    data=[
                        {
                            'parent_id': scheduledmedia.id,
                            'label': 'full',
                            'data': prisma.Base64.encode(image_content)
                        },
                        {
                            'parent_id': scheduledmedia.id,
                            'label': 'thumbnail',
                            'data': prisma.Base64.encode(data_to_thumbnail(image_content))
                        }
                    ]
                )
        del data
        del image_content
        del image
        print('garbage:', gc.collect())
        return web.HTTPSeeOther("/schedule")
    
    @routes.post("/schedule/cancel")
    @require_admin_login
    async def schedule_post_cancel(request: web.Request) -> web.Response:
        blob = await request.json()
        id_ = blob['id']
        assert isinstance(id_, int)
        updated = await db.scheduledpost.update_many(
            where={
                'id': id_,
                'status': {'in': ['scheduled', 'attempting']},
            },
            data={
                'status': 'cancelled'
            }
        )
        if updated > 0:
            return web.HTTPOk(text='cancelled post')
        else:
            return web.HTTPNotFound(text='could not cancel post??')
        
    @routes.post("/schedule/post_immediately")
    @require_admin_login
    async def schedule_post_post_immediately(request: web.Request) -> web.Response:
        blob = await request.json()
        id_ = blob['id']
        assert isinstance(id_, int)
        post = await db.scheduledpost.find_first(where={'id': id_, 'status': {'not': 'posted'}})
        if post is None:
            return web.HTTPNotFound(text='could not find unsent scheduledpost with given id')
        updated = await send_post_and_update_db(db, personal_client, post)
        if updated is None or updated.status != 'posted':
            return web.HTTPServerError(text='something went wrong')
        return web.HTTPOk(text='sent post!')

    @routes.post("/schedule/rechedule")
    @require_admin_login
    async def schedule_post_reschedule(request: web.Request) -> web.Response:
        blob = await request.json()
        id_ = blob['id']
        assert isinstance(id_, int)
        updated = await db.scheduledpost.update_many(
            where={
                'id': id_,
                'status': {'in': ['cancelled', 'failed']},
            },
            data={
                'status': 'scheduled',
                'scheduled_at': datetime.now()
            }
        )
        if updated > 0:
            return web.HTTPOk(text='rescheduled post')
        else:
            return web.HTTPNotFound(text='could not reshedule post??')
        
    @routes.get("/db/images/{imgid}")
    @require_admin_login
    async def db_image(request: web.Request) -> web.Response:
        imgid = int(request.match_info.get('imgid', '-1'))
        img = await db.mediablob.find_first(where={'id': imgid})
        if img is None:
            return web.HTTPNotFound(text='no image with that id')
        return web.HTTPOk(body=img.data.decode(), content_type='image')

    return routes

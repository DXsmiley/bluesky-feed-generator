import asyncio
import sys
import os
import signal
    
from server import config
from server import data_stream
import server.interface

from aiohttp import web

import server.algos
from server.data_filter import operations_callback
from server.algos.score_task import score_posts_forever

import server.load_known_furries

import prisma
import server.database
from server.database import Database, make_database_connection

from typing import AsyncIterator, Callable, Coroutine, Any, Optional, Set

import traceback
import termcolor
from termcolor import cprint
from datetime import datetime
from dataclasses import dataclass

import scripts.find_furry_girls


algos = {
    **{
        (os.environ[server.algos.environment_variable_name_for(i['record_name'])]): i['handler']
        for i in server.algos.algo_details
    },
    **{
        i['record_name']: i['handler']
        for i in server.algos.algo_details
    }
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


def create_and_run_webapp(*, port: int, db_url: Optional[str], services: Optional[Services] = None) -> None:
    asyncio.run(_create_and_run_webapp(port, db_url, services or Services()))


async def _create_and_run_webapp(port: int, db_url: Optional[str], services: Services) -> None:
    db = await make_database_connection(db_url, log_queries=services.log_db_queries)
    app = create_web_application(db, services)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    # Should probably be "while not gotten shutdown signal"
    while True:
        await asyncio.sleep(10)


def create_web_application(db: Database, services: Services) -> web.Application:
    app = web.Application()
    app.add_routes(create_route_table(db, admin_panel=services.admin_panel))
    app.cleanup_ctx.append(background_tasks(db, services))
    return app


def background_tasks(db: Database, services: Services) -> Callable[[web.Application], AsyncIterator[None]]:
    async def catch(name: str, c: Coroutine[Any, Any, None]) -> None:
        try:
            await c
        except KeyboardInterrupt:
            pass
        except:
            termcolor.cprint(f'--------[ Failure in {name} ]--------', 'red', force_color=True)
            termcolor.cprint( 'Critical exception in background task', 'red', force_color=True)
            termcolor.cprint('-------------------------------------', 'red', force_color=True)
            traceback.print_exc()
            termcolor.cprint('-------------------------------------', 'red', force_color=True)
    async def f(_: web.Application) -> AsyncIterator[None]:
        if services.scraper:
            asyncio.create_task(catch('LOADDB', server.load_known_furries.rescan_furry_accounts_forever(db)))
        if services.scores:
            asyncio.create_task(catch('SCORES', score_posts_forever(db)))
        if services.firehose:
            asyncio.create_task(catch('FIREHS', data_stream.run(db, config.SERVICE_DID, operations_callback, None)))
        yield
    return f


def create_route_table(db: Database, *, admin_panel: bool=False):

    routes = web.RouteTableDef()

    routes.static('/static', './static')

    @routes.get('/')
    async def index(request: web.Request) -> web.StreamResponse:
        return web.FileResponse('index.html')


    @routes.get('/stats')
    async def stats(request: web.Request) -> web.Response:
        page = server.interface.stats_page([
            ('feeds', len(server.algos.algo_details)),
            ('users', await db.actor.count()),
            ('in-fox-feed', await db.actor.count(where=server.database.user_is_in_fox_feed)),
            ('in-vix-feed', await db.actor.count(where=server.database.user_is_in_vix_feed)),
            ('storing-data-for', await db.actor.count(where=server.database.care_about_storing_user_data_preemptively)),
            ('posts', await db.post.count()),
            ('likes', await db.like.count()),
            ('postscores', await db.postscore.count()),
        ])
        return web.Response(text=str(page), content_type='text/html')


    @routes.get('/user/{handle}')
    async def user_deets(request: web.Request) -> web.Response:
        handle = request.match_info['handle']
        if not isinstance(handle, str):
            return web.HTTPBadRequest(text='requires parameter "handle"')
        user = await db.actor.find_first(where={'handle': handle})
        if user is None:
            return web.HTTPNotFound(text='user not found')
        posts = await db.post.find_many(where={'authorId': user.did}, order={'indexed_at': 'desc'})
        page = server.interface.user_page(user, posts)
        return web.Response(text=str(page), content_type='text/html')


    @routes.get('/.well-known/did.json')
    async def did_json(request: web.Request) -> web.Response:
        if not config.SERVICE_DID.endswith(config.HOSTNAME):
            return web.HTTPNotFound()

        return web.json_response({
            '@context': ['https://www.w3.org/ns/did/v1'],
            'id': config.SERVICE_DID,
            'service': [
                {
                    'id': '#bsky_fg',
                    'type': 'BskyFeedGenerator',
                    'serviceEndpoint': f'https://{config.HOSTNAME}'
                }
            ]
        })


    @routes.get('/xrpc/app.bsky.feed.describeFeedGenerator')
    async def describe_feed_generator(request: web.Request) -> web.Response:
        feeds = [{'uri': uri} for uri in algos.keys()]
        response = {
            'encoding': 'application/json',
            'body': {
                'did': config.SERVICE_DID,
                'feeds': feeds
            }
        }
        return web.json_response(response)


    @routes.get('/xrpc/app.bsky.feed.getFeedSkeleton')
    async def get_feed_skeleton(request: web.Request) -> web.Response:
        feed = request.query.get('feed', default='')
        algo = algos.get(feed)
        if not algo:
            return web.HTTPBadRequest(text='Unsupported algorithm')
        
        cprint(f'Getting feed {feed}', 'magenta', force_color=True)

        s = datetime.now()

        try:
            cursor = request.query.get('cursor', default=None)
            limit = request.query.get('limit', default=20)
            body = await algo(db, cursor, int(limit))
        except ValueError:
            return web.HTTPBadRequest(text='Malformed Cursor')
        
        d = datetime.now() - s
        
        cprint(f'Done in {int(d.total_seconds())}', 'magenta', force_color=True)

        return web.json_response(body)


    @routes.get('/feed')
    async def get_feeds(request: web.Request) -> web.Response:
        page = server.interface.feeds_page([i['record_name'] for i in server.algos.algo_details])
        return web.Response(text=str(page), content_type='text/html')

    
    @routes.get('/feed/{feed}')
    async def get_feed(request: web.Request) -> web.Response:
        feed_name = request.match_info.get('feed', '')
        algo = algos.get(feed_name)
        if algo is None:
            return web.HTTPNotFound(text='Feed not found')
        
        posts = (await algo(db, None, 50))['feed']
        full_posts = [await db.post.find_unique_or_raise({'uri': i['post']}, include={'author': True}) for i in posts]

        page = server.interface.feed_page(feed_name, full_posts)

        return web.Response(text=str(page), content_type='text/html')


    async def quickflag_candidates_from_feed(feed_name: server.algos.FeedName) -> Set[str]:
        max_version = await db.postscore.find_first_or_raise(where={'feed_name': feed_name}, order={'version': 'desc'})
        postscores = await db.postscore.find_many(where={'feed_name': feed_name, 'version': max_version.version})
        posts = await db.post.find_many(where={'uri': {'in': [i.uri for i in postscores]}})
        return set(i.authorId for i in posts)


    @routes.get('/quickflag')
    async def quickflag(request: web.Request) -> web.Response:
        # pick from feeds that are actually able to contain non-girls
        dids = (
            await quickflag_candidates_from_feed('vix-votes')
        )
        users = await db.actor.find_many(
            take=10,
            where={
                'OR': [
                    {
                        # People who have been marked for reivew, obviously
                        'flagged_for_manual_review': True,
                    },
                    {
                        # People who are hitting the V^2 algo who we might want to include in the Vix Feed
                        # but we can't really discern any information about
                        'did': {'in': list(dids)},
                        'autolabel_fem_vibes': False,
                        'autolabel_masc_vibes': False,
                        'autolabel_nb_vibes': False,
                        'manual_include_in_fox_feed': None,
                        'manual_include_in_vix_feed': None,
                    }
                ]
            },
            include={
                'posts': {
                    'take': 4,
                    'order_by': {'indexed_at': 'desc'},
                }
            }
        )
        page = server.interface.quickflag_page(users)
        return web.Response(text=str(page), content_type='text/html')


    @routes.post('/admin/mark')
    async def mark_user(request: web.Request) -> web.Response:
        if not admin_panel:
            return web.HTTPForbidden(text='admin tools currently disabled')
        blob = await request.json()
        print(blob)
        did = blob['did']
        assert isinstance(did, str)
        action: prisma.types.ActorUpdateInput = {'flagged_for_manual_review': False}
        if 'include_in_fox_feed' in blob:
            action['manual_include_in_fox_feed'] = blob['include_in_fox_feed']
        if 'include_in_vix_feed' in blob:
            action['manual_include_in_vix_feed'] = blob['include_in_vix_feed']
        updated = await db.actor.update(
            where={'did': did},
            data=action,
        )
        if updated is None:
            return web.HTTPNotFound(text='user not found')
        return web.HTTPOk(text=f'{updated.handle} assigned to fox:{updated.manual_include_in_fox_feed}, vix:{updated.manual_include_in_vix_feed}')


    @routes.post('/admin/scan_likes')
    async def scan_likes(request: web.Request) -> web.Response:
        if not admin_panel:
            return web.HTTPForbidden(text='admin tools currently disabled')
        blob = await request.json()
        print(blob)
        uri = blob['uri']
        assert isinstance(uri, str)
        added_users, added_likes = await scripts.find_furry_girls.from_likes_of_post(db, uri)
        return web.HTTPOk(text=f'Found {added_users} new candidate furries and {added_likes} new likes')


    return routes

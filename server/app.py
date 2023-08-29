import asyncio
import sys
import os
import signal
    
from server import config
from server import data_stream

from aiohttp import web

import server.algos
from server.data_filter import operations_callback
from server.algos.score_task import score_posts_forever

import server.load_known_furries

from server.database import Database, make_database_connection

from typing import AsyncIterator, Callable, Coroutine, Any

import traceback
import termcolor


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


def create_and_run_webapp(port: int) -> None:
    asyncio.run(_create_and_run_webapp(port))


async def _create_and_run_webapp(port: int) -> None:
    db = await make_database_connection()
    app = create_web_application(db)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    # Should probably be "while not gotten shutdown signal"
    while True:
        await asyncio.sleep(10)


def create_web_application(db: Database) -> web.Application:
    app = web.Application()
    app.add_routes(create_route_table(db))
    app.cleanup_ctx.append(background_tasks(db))
    return app


def background_tasks(db: Database) -> Callable[[web.Application], AsyncIterator[None]]:
    async def catch(name: str, c: Coroutine[Any, Any, None]) -> None:
        try:
            await c
        except:
            termcolor.cprint(f'--------[ Failure in {name} ]--------', 'red', force_color=True)
            termcolor.cprint( 'Critical exception in background task', 'red', force_color=True)
            termcolor.cprint('-------------------------------------', 'red', force_color=True)
            traceback.print_exc()
            termcolor.cprint('-------------------------------------', 'red', force_color=True)
    async def f(_: web.Application) -> AsyncIterator[None]:
        asyncio.create_task(catch('LOADDB', server.load_known_furries.load(db)))
        asyncio.create_task(catch('SCORES', score_posts_forever(db)))
        asyncio.create_task(catch('FIREHS', data_stream.run(db, config.SERVICE_DID, operations_callback, None)))
        yield
    return f


def create_route_table(db: Database):

    routes = web.RouteTableDef()

    @routes.get('/')
    async def index(request: web.Request) -> web.StreamResponse:
        return web.FileResponse('index.html')


    @routes.get('/stats')
    async def stats(request: web.Request) -> web.Response:
        users = await db.actor.count()
        posts = await db.post.count()
        postscores = await db.postscore.count()
        return web.Response(text=f'''
            DB stats:<br>
            {users} users<br>
            {posts} posts<br>
            {postscores} postscores<br>
        ''', content_type='text/html')


    # TODO: double check this
    def htmlescape(s: str) -> str:
        return s.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;').replace('\n', '')

    @routes.get('/user-deets')
    async def user_deets(request: web.Request) -> web.Response:
        handle = request.query.get('handle', None)
        if not isinstance(handle, str):
            return web.HTTPBadRequest(text='requires parameter "handle"')
        user = await db.actor.find_first(where={'handle': handle})
        if user is None:
            return web.HTTPNotFound(text='user not found')
        posts = await db.post.find_many(where={'authorId': user.did})
        posts_html = ''.join([
            f"<code>{i.uri}</code><br>{i.media_count}M {i.like_count}L // {htmlescape(i.text)}<br><br>"
            for i in posts
        ])
        return web.Response(text=f'''
            {handle} {htmlescape(user.displayName or '')}<br>
            Fox feed: {user.in_fox_feed}<br>
            Vix feed: {user.in_vix_feed}<br>
            <br>
            {len(posts)} posts in db<br><br>
            {posts_html}<br>
        ''', content_type='text/html')

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

        try:
            cursor = request.query.get('cursor', default=None)
            limit = request.query.get('limit', default=20)
            body = await algo(db, cursor, int(limit))
        except ValueError:
            return web.HTTPBadRequest(text='Malformed Cursor')

        return web.json_response(body)
    
    @routes.get('/feed/{feed}')
    async def get_feed(request: web.Request) -> web.Response:
        feed_name = request.match_info['feed']
        algo = algos.get(feed_name)
        if algo is None:
            return web.HTTPNotFound(text='Feed not found')
        
        posts = (await algo(db, None, 50))['feed']

        full_posts = [await db.post.find_unique_or_raise({'uri': i['post']}, include={'author': True}) for i in posts]

        t = '<br>'.join(
            f'{"?" if not i.author else i.author.handle} - {i.like_count}L - {i.media_count}M - {i.text}'
            for i in full_posts
        )
        
        return web.Response(text=f'<html><body><h3>{feed_name}</h3><br>{t}</body></html>', content_type='text/html')

    
    return routes

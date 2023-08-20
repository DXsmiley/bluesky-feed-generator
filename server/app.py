import sys
import os
import signal
import threading

from server import config
from server import data_stream

from flask import Flask, jsonify, request

import server.algos
from server.data_filter import operations_callback
from server.algos.score_task import score_posts_forever

import server.load_known_furries

from server.database import db

from typing import Any
from typing_extensions import Never

app = Flask(__name__)

stream_stop_event = threading.Event()
stream_thread = threading.Thread(
    target=data_stream.run, args=(config.SERVICE_DID, operations_callback, stream_stop_event,)
)
stream_thread.start()

load_furries_thread = threading.Thread(
    target=server.load_known_furries.load, args=()
)
load_furries_thread.start()

score_posts_thread = threading.Thread(
    target=score_posts_forever, args=()
)
score_posts_thread.start()


algos = {
    (os.environ[server.algos.environment_variable_name_for(i['record_name'])]): i['handler']
    for i in server.algos.algo_details
}


def sigint_handler(*_: Any) -> Never:
    print('Stopping data stream...')
    stream_stop_event.set()
    sys.exit(0)


signal.signal(signal.SIGINT, sigint_handler)


@app.route('/')
def index():
    return open('index.html').read()


@app.route('/stats')
def stats():
    users = db.actor.count()
    posts = db.post.count()
    return f'''
        DB stats:<br>
        {users} users<br>
        {posts} posts
    '''


@app.route('/.well-known/did.json', methods=['GET'])
def did_json():
    if not config.SERVICE_DID.endswith(config.HOSTNAME):
        return '', 404

    return jsonify({
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


@app.route('/xrpc/app.bsky.feed.describeFeedGenerator', methods=['GET'])
def describe_feed_generator():
    feeds = [{'uri': uri} for uri in algos.keys()]
    response = {
        'encoding': 'application/json',
        'body': {
            'did': config.SERVICE_DID,
            'feeds': feeds
        }
    }
    return jsonify(response)


@app.route('/xrpc/app.bsky.feed.getFeedSkeleton', methods=['GET'])
def get_feed_skeleton():
    feed = request.args.get('feed', default='', type=str)
    algo = algos.get(feed)
    if not algo:
        return 'Unsupported algorithm', 400

    try:
        cursor = request.args.get('cursor', default=None, type=str)
        limit = request.args.get('limit', default=20, type=int)
        body = algo(cursor, limit)
    except ValueError:
        return 'Malformed cursor', 400

    return jsonify(body)

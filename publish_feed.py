#!/usr/bin/env python3
# YOU MUST INSTALL ATPROTO SDK
# pip3 install atproto

import os
import dotenv
from datetime import datetime
from atproto.xrpc_client.models import ids
from atproto import Client, models
from typing import Optional
from server.algos import algo_details, environment_variable_name_for

dotenv.load_dotenv()

# YOUR bluesky handle
# Ex: user.bsky.social
HANDLE: str = os.environ['HANDLE']

# YOUR bluesky password, or preferably an App Password (found in your client settings)
# Ex: abcd-1234-efgh-5678
PASSWORD: str = os.environ['PASSWORD']

# The hostname of the server where feed server will be hosted
# Ex: feed.bsky.dev
HOSTNAME: str = os.environ['HOSTNAME']


# (Optional). Only use this if you want a service did different from did:web
SERVICE_DID: Optional[str] = None


def register(client: Client, record_name: str, display_name: str, description: str, avatar_path: Optional[str]):
    feed_did = SERVICE_DID if SERVICE_DID is not None else f'did:web:{HOSTNAME}'

    avatar_blob = None
    if avatar_path:
        with open(avatar_path, 'rb') as f:
            avatar_data = f.read()
            avatar_blob = client.com.atproto.repo.upload_blob(avatar_data).blob

    response = client.com.atproto.repo.put_record(models.ComAtprotoRepoPutRecord.Data(
        repo=client.me.did,
        collection=ids.AppBskyFeedGenerator,
        rkey=record_name,
        record=models.AppBskyFeedGenerator.Main(
            did=feed_did,
            displayName=display_name,
            description=description,
            avatar=avatar_blob,
            createdAt=datetime.now().isoformat(),
        )
    ))

    return response.uri


def main():
    client = Client()
    client.login(HANDLE, PASSWORD)

    for i in algo_details:
        uri = register(client, i['record_name'], i['display_name'], i['description'], './fox.png')
        env_variable_name = environment_variable_name_for(i['record_name'])
        print(f'{env_variable_name}="{uri}"')


if __name__ == '__main__':
    main()

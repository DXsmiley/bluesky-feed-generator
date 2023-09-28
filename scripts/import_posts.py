import asyncio
import sys
from foxfeed.database import make_database_connection
from typing import List, Literal, Optional
from pydantic import BaseModel
from prisma import Base64


LABEL_VALUES = Literal['sexual', 'nudity', 'porn', 'nsfl']


class Media(BaseModel):
    alt_text: str
    path: str


class Post(BaseModel):
    text: str
    media: List[Media] = []
    label: Optional[LABEL_VALUES] = None


class Blob(BaseModel):
    posts: List[Post]


async def main(filename: str):
    db = await make_database_connection()
    blob = Blob.model_validate_json(open(filename).read())
    print(blob.model_dump_json(indent=4))
    for post in blob.posts:
        r = await db.scheduledpost.create(
            include={'media': True},
            data={
                'text': post.text,
                'label': post.label,
                'media': {
                    'create': [
                        {
                            'alt_text': media.alt_text,
                            'data': Base64.encode(open(media.path, 'rb').read())
                        }
                        for media in post.media
                    ]
                }
            }
        )
        print(r)


if __name__ == '__main__':
    assert len(sys.argv) == 2
    filename = sys.argv[1]
    asyncio.run(main(filename))

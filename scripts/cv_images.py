# shitass monkeypatch
import tensorflow as tf
tf.gfile = tf.io.gfile

import asyncio
from server.database import make_database_connection
from PIL import Image
from aiohttp import ClientSession
from fursuit_model.model import FursuitModel
import io
import numpy as np
from typing import List, TypedDict
from prisma.fields import Json
import traceback


KEEP_BOX_THRESHOLD = 0.4
IMAGE_HAS_FURSUIT_THRESHOLD = 0.95


async def download_image(url: str) -> Image.Image:
    async with ClientSession() as session:
        response = await session.get(url)
        data = await response.read()
        return Image.open(io.BytesIO(data)).convert('RGB')
    

class ImageCVBox(TypedDict):
    name: str
    score: float
    x1: float
    y1: float
    x2: float
    y2: float


async def classify_image(model: FursuitModel, url: str) -> List[ImageCVBox]:
    image = await download_image(url)
    r = model.run_model(np.array(image))
    boxes = [
        ImageCVBox(
            name=model._category_index[class_]['name'],
            score=float(score),
            x1=float(x1),
            y1=float(y1),
            x2=float(x2),
            y2=float(y2),
        )
        for (y1, x1, y2, x2), class_, score in zip(r['detection_boxes'], r['detection_classes'], r['detection_scores'])
        if score >= KEEP_BOX_THRESHOLD
    ]
    return boxes


async def main():
    model = FursuitModel()
    db = await make_database_connection()
    for _ in range(100):
        post = await db.post.find_first(
            order={'indexed_at': 'desc'},
            where={
                'cv_model_name': None,
                'media_count': {'gt': 0},
                'NOT': [{'m0': None}],
            },
            include={'author': True},
        )
        if post is None:
            break
        try:
            results = [
                await classify_image(model, i)
                for i in [post.m0, post.m1, post.m2, post.m3]
                if i is not None
            ]
            has_fursuit = any(
                j['score'] >= IMAGE_HAS_FURSUIT_THRESHOLD
                for i in results
                for j in i
            )
            print(results)
            await db.post.update(
                where={'uri': post.uri},
                data={
                    'cv_bounding_boxes': Json({'boxes': results}),
                    'cv_has_fursuit': has_fursuit,
                    'cv_model_name': 'zenith-v0',
                }
            )
        except:
            traceback.print_exc()
            await db.post.update(
                where={'uri': post.uri},
                data={
                    'cv_model_name': 'error',
                }
            )


asyncio.run(main())



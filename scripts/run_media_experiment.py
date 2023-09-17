import asyncio
from server.database import make_database_connection, Post
from typing import List, Tuple
from aiohttp import ClientSession
from PIL import Image
from dataclasses import dataclass
import io
import numpy as np
import traceback
from prisma.types import ExperimentResultCreateWithoutRelationsInput
import scipy.signal
import math


@dataclass
class Result:
    score: float
    comment: str
    did_error: bool


EXPERIMENT_NAME = 'estimate-noise'
EXPERIMENT_VERSION = 2


async def download_image(url: str) -> Image.Image:
    async with ClientSession() as session:
        response = await session.get(url)
        data = await response.read()
        return Image.open(io.BytesIO(data)).convert('RGB')
    

def sharp_edges(img: Image.Image) -> float:
    img = img.convert('L')
    values = abs(np.fft.fft2(np.asarray(img.convert('L')))).flatten().tolist()
    high_values = [x for x in values if x > 1_000]
    high_values_ratio = 100*(float(len(high_values))/len(values))
    return high_values_ratio


def estimate_noise(img: Image.Image) -> float:
    I = np.asarray(img.convert('L'))
    H, W = I.shape

    M = [[1, -2, 1],
        [-2, 4, -2],
        [1, -2, 1]]

    sigma = np.sum(np.sum(np.absolute(scipy.signal.convolve2d(I, M))))
    sigma = sigma * math.sqrt(0.5 * math.pi) / (6 * (W-2) * (H-2))

    return sigma


async def run_experiment_on_image(url: str) -> Result:
    try:
        image = await download_image(url)
        score = estimate_noise(image)
        print('Score:', score)
        return Result(
            score=score,
            comment='',
            did_error=False
        )
    except KeyboardInterrupt:
        raise
    except Exception as e:
        traceback.print_exc()
        return Result(
            score=0,
            comment='error: ' + str(e),
            did_error=True
        )


async def run_experiment_on_post(post: Post) -> List[Tuple[str, int, Result]]:
    return [
        (post.uri, i, await run_experiment_on_image(url))
        for i, url in enumerate([post.m0, post.m1, post.m2, post.m3])
        if url is not None
    ]


async def main():
    db = await make_database_connection()
    while True:
        posts = await db.post.find_many(
            take=20,
            where={
                'media_count': {'gt': 0},
                'experiment_results':
                    {
                        'none':
                            {
                                'experiment_name': EXPERIMENT_NAME,
                                'experiment_version': EXPERIMENT_VERSION
                            }
                    }
            }
        )
        results = [
            j
            for i in posts
            for j in await run_experiment_on_post(i)
        ]
        blob: List[ExperimentResultCreateWithoutRelationsInput] = [
            {
                'post_uri': post_uri,
                'media_index': media_index,
                'experiment_name': EXPERIMENT_NAME,
                'experiment_version': EXPERIMENT_VERSION,
                'result_score': result.score,
                'result_comment': result.comment,
                'did_error': result.did_error
            }
            for post_uri, media_index, result in results
        ]
        await db.experimentresult.create_many(data=blob)


if __name__ == '__main__':
    asyncio.run(main())

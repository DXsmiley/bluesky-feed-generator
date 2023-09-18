import jwt
import multibase
import aiohttp
from server.config import SERVICE_DID
from typing import Optional, Callable, Coroutine, Any, Tuple
from collections import OrderedDict
from time import time

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ec import (
    SECP256K1,
    EllipticCurvePublicKey,
)


class TimedCache:

    def __init__(self, function: Callable[[str], Coroutine[Any, Any, str]]):
        self.function = function
        self.cache: OrderedDict[str, Tuple[float, str]] = OrderedDict()

    async def __call__(self, key: str) -> str:
        curtime = time()
        if key in self.cache:
            cache_time, cache_value = self.cache[key]
            if curtime - cache_time < 60 * 30:
                return cache_value
        new_value = await self.function(key)
        self.cache[key] = (curtime, new_value)
        while len(self.cache) > 200:
            self.cache.popitem(False)
        return new_value


@TimedCache
async def get_pubkey_from_server(did: str) -> str:
    async with aiohttp.ClientSession() as session:
        response = await session.get(f"https://plc.directory/{did}")
        blob = await response.json()
        # TODO: allow multiple keys
        pubkey_multibase = blob["verificationMethod"][0]["publicKeyMultibase"]
    return pubkey_multibase


async def verify_jwt(bearer: Optional[str]) -> Optional[str]:
    if bearer is None or not bearer.startswith("Bearer "):
        return None
    token = bearer[7:]

    did = jwt.decode(
        token, algorithms=["ES256K"], options={"verify_signature": False}
    ).get("iss")

    pubkey_multibase = await get_pubkey_from_server(did)

    decoded = multibase.decode(pubkey_multibase)
    assert isinstance(decoded, bytes)

    ecpk = EllipticCurvePublicKey.from_encoded_point(SECP256K1(), decoded)
    key = ecpk.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )

    return jwt.decode(token, key, audience=SERVICE_DID, algorithms=["ES256K"]).get("iss")

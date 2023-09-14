import jwt
import multibase
import aiohttp
from server.config import SERVICE_DID
from typing import Optional

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ec import (
    SECP256K1,
    EllipticCurvePublicKey,
)


async def verify_jwt(bearer: Optional[str]) -> Optional[str]:
    if bearer is None or not bearer.startswith("Bearer "):
        return None
    token = bearer[7:]

    did = jwt.decode(
        token, algorithms=["ES256K"], options={"verify_signature": False}
    ).get("iss")

    async with aiohttp.ClientSession() as session:
        response = await session.get(f"https://plc.directory/{did}")
        blob = await response.json()
        # TODO: allow multiple keys
        pubkey_multibase = blob["verificationMethod"][0]["publicKeyMultibase"]

    decoded = multibase.decode(pubkey_multibase)
    assert isinstance(decoded, bytes)

    ecpk = EllipticCurvePublicKey.from_encoded_point(SECP256K1(), decoded)
    key = ecpk.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )

    return jwt.decode(token, key, audience=SERVICE_DID, algorithms=["ES256K"]).get("iss")

import jwt
import multibase  # type: ignore
import aiohttp
from foxfeed.config import SERVICE_DID
from typing import Optional, Callable, Coroutine, Any, Tuple, TypeVar, Generic, List
from collections import OrderedDict
from time import time
from pydantic import BaseModel
import exceptiongroup

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ec import (
    SECP256K1,
    SECP256R1,
    EllipticCurvePublicKey,
)


T = TypeVar("T")


class TimedCache(Generic[T]):
    def __init__(self, function: Callable[[str], Coroutine[Any, Any, T]]):
        self.function = function
        self.cache: OrderedDict[str, Tuple[float, T]] = OrderedDict()

    async def __call__(self, key: str) -> T:
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


class VerificationMethod(BaseModel):
    id: str
    type: str
    controller: str
    publicKeyMultibase: str


@TimedCache
async def get_verification_methods(did: str) -> List[VerificationMethod]:
    async with aiohttp.ClientSession() as session:
        response = await session.get(f"https://plc.directory/{did}")
        blob = await response.json()
        return [VerificationMethod(**i) for i in blob["verificationMethod"]]


def run_verification_method(token: str, vm: VerificationMethod) -> None:
    if vm.type == "Multikey":
        decoded = multibase.decode(vm.publicKeyMultibase)  # type: ignore
        assert isinstance(decoded, bytes)

        curve_code = decoded[:2]
        key_bytes = decoded[2:]

        curve = (
            SECP256R1()
            if curve_code == b"\x80\x24"
            else SECP256K1()
            if curve_code == b"\xe7\x01"
            else None
        )

        if curve is None:
            raise ValueError(f"Unknown curve code bytes: {curve_code}")

        ecpk = EllipticCurvePublicKey.from_encoded_point(curve, key_bytes)
        key = ecpk.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        )

        jwt.decode(token, key, audience=SERVICE_DID, algorithms=["ES256K"])

    elif vm.type == "EcdsaSecp256k1VerificationKey2019":
        decoded = multibase.decode(vm.publicKeyMultibase)  # type: ignore
        assert isinstance(decoded, bytes)

        ecpk = EllipticCurvePublicKey.from_encoded_point(SECP256K1(), decoded)
        key = ecpk.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        )

        jwt.decode(token, key, audience=SERVICE_DID, algorithms=["ES256K"])

    else:
        raise ValueError("Unknown verification method type: " + vm.type)


async def verify_jwt(bearer: Optional[str]) -> Optional[str]:
    if bearer is None or not bearer.startswith("Bearer "):
        return None
    token = bearer[7:]

    did = jwt.decode(
        token, algorithms=["ES256K"], options={"verify_signature": False}
    ).get("iss")

    vms = await get_verification_methods(did)

    if not vms:
        raise ValueError("No verification methods were provided")

    success_count = 0
    exceptions: List[Exception] = []
    for vm in vms:
        try:
            # If there's a problem, this will raise, or return normally if the JWT got verified
            run_verification_method(token, vm)
            success_count += 1
        except Exception as e:
            exceptions.append(e)

    if success_count > 0:
        return did

    raise exceptiongroup.ExceptionGroup("jwt verification failure", exceptions)

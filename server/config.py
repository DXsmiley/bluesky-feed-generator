import os
import dotenv

dotenv.load_dotenv()

_service_did = os.environ.get('SERVICE_DID', None)
_hostname = os.environ.get('HOSTNAME', None)

if _hostname is None:
    raise RuntimeError('You should set "HOSTNAME" environment variable first.')

HOSTNAME: str = _hostname

SERVICE_DID: str = f'did:web:{HOSTNAME}' if _service_did is None else _service_did

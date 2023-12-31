import os
import dotenv
from typing import Optional

dotenv.load_dotenv()


def value(key: str, default: Optional[str] = None) -> str:
    v = os.environ.get(key, default)
    if not isinstance(v, str):
        raise RuntimeError(f'Environment variable {key} not set')
    return v


HOSTNAME: str = value('HOSTNAME')
SERVICE_DID: str = value("SERVICE_DID", f"did:web:{HOSTNAME}")
PORT: int = int(value("PORT", "8000"))
DB_URL: Optional[str] = os.environ.get("DATABASE_URL")

HANDLE = value('HANDLE')
PASSWORD = value('PASSWORD')

PERSONAL_HANDLE = value('PERSONAL_HANDLE')
PERSONAL_PASSWORD = value('PERSONAL_PASSWORD')

ADMIN_PANEL_PASSWORD: Optional[str] = os.environ.get('ADMIN_PANEL_PASSWORD')


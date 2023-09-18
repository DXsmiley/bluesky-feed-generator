import atproto
import atproto.exceptions
from server.config import HANDLE, PASSWORD
from server.database import Database


AsyncClient = atproto.AsyncClient


async def _login_from_handle_and_password() -> AsyncClient:
    print('Attempting login with handle and password')
    client = AsyncClient()
    await client.login(login=HANDLE, password=PASSWORD)
    return client


async def _login_from_session_string(string: str) -> AsyncClient:
    print('Attempting login using session string')
    client = AsyncClient()
    await client.login(session_string=string)
    return client


async def _login_from_best_source(db: Database) -> AsyncClient:
    session = await db.blueskyclientsession.find_first(
        where={'handle': HANDLE},
        order={'created_at': 'desc'},
    )
    try:
        assert session is not None
        return await _login_from_session_string(session.session_string)
    except:
        client = await _login_from_handle_and_password()
        await db.blueskyclientsession.create(
            data={
                'handle': HANDLE,
                'session_string': client.export_session_string()
            }
        )
        return client


async def make_bsky_client(db: Database) -> AsyncClient:
    return await _login_from_best_source(db)

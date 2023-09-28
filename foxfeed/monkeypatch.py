from atproto.xrpc_client.models.com.atproto.server import create_session
from atproto.xrpc_client.models import base
from pydantic import Field
import typing as t

class Response(base.ResponseModelBase):

    """Output data model for :obj:`com.atproto.server.createSession`."""

    access_jwt: str = Field(alias='accessJwt')  #: Access jwt.
    did: str  #: Did.
    handle: str  #: Handle.
    refresh_jwt: str = Field(alias='refreshJwt')  #: Refresh jwt.
    email: t.Optional[str] = None  #: Email.
    emailConfirmed: t.Optional[bool] = None

create_session.Response = Response

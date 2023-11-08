raise Exception('deprecated file, keeping it for reference, do not use')

import asyncio
import random
import socket
import traceback
import typing as t
from copy import deepcopy
from urllib.parse import urlencode

from websockets.client import connect as aconnect
from websockets.exceptions import (
    ConnectionClosedError,
    ConnectionClosedOK,
    InvalidHandshake,
    PayloadTooBig,
    ProtocolError,
)

from atproto.exceptions import DAGCBORDecodingError, FirehoseDecodingError, FirehoseError
from atproto.firehose.models import ErrorFrame, Frame, MessageFrame
from atproto.xrpc_client import models
from atproto.xrpc_client.models.common import XrpcError

from foxfeed.util import sleep_on


_BASE_WEBSOCKET_URI = 'wss://bsky.social/xrpc'
_MAX_MESSAGE_SIZE_BYTES = 1024 * 1024 * 5  # 5MB

OnMessageCallback = t.Callable[['MessageFrame'], t.Generator[t.Any, None, t.Any]]
AsyncOnMessageCallback = t.Callable[['MessageFrame'], t.Coroutine[t.Any, t.Any, t.Any]]

OnCallbackErrorCallback = t.Callable[[BaseException], None]


def _build_websocket_uri(
    method: str, base_uri: t.Optional[str] = None, params: t.Optional[t.Dict[str, t.Any]] = None
) -> str:
    if base_uri is None:
        base_uri = _BASE_WEBSOCKET_URI

    query_string = ''
    if params:
        query_string = f'?{urlencode(params)}'

    return f'{base_uri}/{method}{query_string}'


def _handle_frame_decoding_error(exception: Exception) -> None:
    if isinstance(exception, (DAGCBORDecodingError, FirehoseDecodingError)):
        # Ignore an invalid firehose frame that could not be properly decoded.
        # It's better to ignore one frame rather than stop the whole connection
        # or trap into an infinite loop of reconnections.
        return

    raise exception


def _print_exception(exception: BaseException) -> None:
    traceback.print_exception(type(exception), exception, exception.__traceback__)


def _handle_websocket_error_or_stop(exception: Exception) -> bool:
    """Returns if the connection should be properly being closed or reraise exception."""
    if isinstance(exception, (ConnectionClosedOK,)):
        return True
    if isinstance(exception, (ConnectionClosedError, InvalidHandshake, PayloadTooBig, ProtocolError, socket.gaierror)):
        return False
    if isinstance(exception, FirehoseError):
        raise exception

    raise FirehoseError from exception


class AsyncFirehoseClient:
    def __init__(
        self, method: str, base_uri: t.Optional[str] = None, params: t.Optional[t.Dict[str, t.Any]] = None
    ) -> None:
        self._method = method
        self._base_uri = base_uri
        self._params = params

        self._reconnect_no = 0
        self._max_reconnect_delay_sec = 64

        self._loop = asyncio.get_event_loop()
        self._stop_event = asyncio.Event()

        self._on_message_callback: t.Optional[AsyncOnMessageCallback] = None

    def update_params(self, params: t.Dict[str, t.Any]) -> None:
        """Update params.

        Warning:
            If you are using `params` arg at the client start, you must care about keeping params up to date.
            Otherwise, your client will be rolled back to the previous state (cursor) on reconnecting.
        """
        self._params = deepcopy(params)

    @property
    def _websocket_uri(self) -> str:
        # the user should care about updated params by himself
        return _build_websocket_uri(self._method, self._base_uri, self._params)

    async def _process_raw_frame(self, data: bytes) -> None:
        frame = Frame.from_bytes(data)
        if isinstance(frame, ErrorFrame):
            raise FirehoseError(XrpcError(frame.body.error, frame.body.message))
        if isinstance(frame, MessageFrame):
            await self._process_message_frame(frame)
        else:
            raise FirehoseDecodingError('Unknown frame type')

    async def _process_message_frame(self, frame: 'MessageFrame') -> None:
        try:
            if self._on_message_callback:
                await self._on_message_callback(frame)
        except Exception as exception:
            if not self._on_callback_error_callback:
                _print_exception(exception)
            else:
                try:
                    self._on_callback_error_callback(exception)
                except:
                    traceback.print_exc()

    def _get_async_client(self):
        return aconnect(self._websocket_uri, max_size=_MAX_MESSAGE_SIZE_BYTES, close_timeout=0.2)
    
    def _get_reconnection_delay(self) -> int:
        base_sec = 2**self._reconnect_no
        rand_sec = random.uniform(-0.5, 0.5)  # noqa: S311

        return min(base_sec, self._max_reconnect_delay_sec) + rand_sec

    async def start(self,
        on_message_callback: AsyncOnMessageCallback,
        on_callback_error_callback: t.Optional[OnCallbackErrorCallback] = None
    ) -> None:
        self._on_message_callback = on_message_callback
        self._on_callback_error_callback = on_callback_error_callback

        self._stop_event = asyncio.Event()

        while not self._stop_event.is_set():
            try:
                if self._reconnect_no != 0:
                    await sleep_on(self._stop_event, self._get_reconnection_delay())

                if self._stop_event.is_set():
                    break

                async with self._get_async_client() as client:
                    self._reconnect_no = 0

                    while not self._stop_event.is_set():
                        raw_frame = await client.recv()
                        if isinstance(raw_frame, str):
                            # skip text frames (should not be occurred)
                            continue

                        try:
                            await self._process_raw_frame(raw_frame)
                        except Exception as e:  # noqa: BLE001
                            _handle_frame_decoding_error(e)
            except Exception as e:  # noqa: BLE001
                self._reconnect_no += 1

                should_stop = _handle_websocket_error_or_stop(e)
                if should_stop:
                    break

    def stop(self):
        self._stop_event.set()


class AsyncFirehoseSubscribeReposClient(AsyncFirehoseClient):
    """Async firehose subscribe repos client.

    Args:
        params: Parameters model.
        base_uri: Base websocket URI. Example: `wss://bsky.social/xrpc`.
    """

    def __init__(
        self,
        params: 'models.ComAtprotoSyncSubscribeRepos.ParamsDict',
        base_uri: t.Optional[str] = None,
    ) -> None:
        
        super().__init__(method='com.atproto.sync.subscribeRepos', base_uri=base_uri, params=dict(params))

import typing as t

import asyncio
from atproto.exceptions import FirehoseDecodingError, FirehoseError
from atproto.firehose.client import _WebsocketClientBase, _print_exception, _handle_frame_decoding_error, AsyncOnMessageCallback, OnCallbackErrorCallback, _handle_websocket_error_or_stop, _MAX_MESSAGE_SIZE_BYTES
from atproto.firehose.models import ErrorFrame, Frame, MessageFrame
from atproto.xrpc_client import models
from atproto.xrpc_client.models.utils import get_model_as_dict, get_or_create
from atproto.xrpc_client.models import get_model_as_dict
from atproto.xrpc_client.models.common import XrpcError
import traceback
from foxfeed.util import sleep_on
from websockets.client import connect as aconnect


if t.TYPE_CHECKING:
    from atproto.firehose.models import MessageFrame


class AsyncFirehoseClient(_WebsocketClientBase):
    def __init__(
        self, method: str, base_uri: t.Optional[str] = None, params: t.Optional[t.Dict[str, t.Any]] = None
    ) -> None:
        super().__init__(method, base_uri, params)

        self._loop = asyncio.get_event_loop()
        self._stop_event = asyncio.Event()

        self._on_message_callback: t.Optional[AsyncOnMessageCallback] = None

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
        params: t.Optional[t.Union[dict, 'models.ComAtprotoSyncSubscribeRepos.Params']] = None,
        base_uri: t.Optional[str] = None,
    ) -> None:
        params_model = get_or_create(params, models.ComAtprotoSyncSubscribeRepos.Params)

        params_dict = None
        if params_model:
            params_dict = get_model_as_dict(params_model)

        super().__init__(method='com.atproto.sync.subscribeRepos', base_uri=base_uri, params=params_dict)

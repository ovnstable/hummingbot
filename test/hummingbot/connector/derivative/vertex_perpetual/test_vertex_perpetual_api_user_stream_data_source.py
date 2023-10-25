import asyncio
import json
import unittest
from typing import Awaitable, Optional
from unittest.mock import AsyncMock, MagicMock, patch

from hummingbot.connector.derivative.vertex_perpetual import (
    vertex_perpetual_constants as CONSTANTS,
    vertex_perpetual_web_utils as web_utils,
)
from hummingbot.connector.derivative.vertex_perpetual.vertex_perpetual_api_user_stream_data_source import (
    VertexPerpetualAPIUserStreamDataSource,
)
from hummingbot.connector.derivative.vertex_perpetual.vertex_perpetual_auth import VertexPerpetualAuth
from hummingbot.connector.test_support.network_mocking_assistant import NetworkMockingAssistant
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler


class TestVertexPerpetualAPIUserStreamDataSource(unittest.TestCase):
    # the level is required to receive logs from the data source logger
    level = 0

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.ev_loop = asyncio.get_event_loop()
        cls.base_asset = "wBTC"
        cls.quote_asset = "USDC"
        cls.trading_pair = f"{cls.base_asset}-{cls.quote_asset}"
        cls.ex_trading_pair = cls.base_asset + cls.quote_asset
        cls.domain = CONSTANTS.TESTNET_DOMAIN

    def setUp(self) -> None:
        super().setUp()

        self.log_records = []
        self.listening_task: Optional[asyncio.Task] = None
        self.mocking_assistant = NetworkMockingAssistant()

        self.throttler = AsyncThrottler(CONSTANTS.RATE_LIMITS)
        self.mock_time_provider = MagicMock()
        self.mock_time_provider.time.return_value = 1000

        # NOTE: RANDOM KEYS GENERATED JUST FOR UNIT TESTS
        self.auth = VertexPerpetualAuth(
            "0x2162Db26939B9EAF0C5404217774d166056d31B5",  # noqa: mock
            "5500eb16bf3692840e04fb6a63547b9a80b75d9cbb36b43ca5662127d4c19c83",  # noqa: mock
        )

        self.api_factory = web_utils.build_api_factory(throttler=self.throttler, auth=self.auth)

        self.data_source = VertexPerpetualAPIUserStreamDataSource(
            auth=self.auth,
            trading_pairs=[self.trading_pair],
            domain=self.domain,
            api_factory=self.api_factory,
            throttler=self.throttler,
        )

        self.data_source.logger().setLevel(1)
        self.data_source.logger().addHandler(self)

    def tearDown(self) -> None:
        self.listening_task and self.listening_task.cancel()
        super().tearDown()

    def handle(self, record):
        self.log_records.append(record)

    def _is_logged(self, log_level: str, message: str) -> bool:
        return any(record.levelname == log_level and record.getMessage() == message for record in self.log_records)

    def async_run_with_timeout(self, coroutine: Awaitable, timeout: int = 1):
        ret = self.ev_loop.run_until_complete(asyncio.wait_for(coroutine, timeout))
        return ret

    # def test_last_recv_time(self):
    #     # Initial last_recv_time
    #     self.assertEqual(0, self.data_source.last_recv_time)

    #     ws_assistant = self.async_run_with_timeout(self.data_source._get_ws_assistant())
    #     ws_assistant._connection._last_recv_time = 1000
    #     self.assertEqual(1000, self.data_source.last_recv_time)

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    def test_listen_for_user_stream_does_not_queue_pong_payload(self, mock_ws):
        mock_pong = {"pong": "1545910590801"}
        mock_ws.return_value = self.mocking_assistant.create_websocket_mock()
        self.mocking_assistant.add_websocket_aiohttp_message(mock_ws.return_value, json.dumps(mock_pong))

        msg_queue = asyncio.Queue()
        self.listening_task = self.ev_loop.create_task(self.data_source.listen_for_user_stream(msg_queue))

        self.mocking_assistant.run_until_all_aiohttp_messages_delivered(mock_ws.return_value)

        self.assertEqual(0, msg_queue.qsize())

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    def test_listen_for_user_stream_does_not_queue_unknown_event(self, mock_ws):
        unknown_event = [{"type": "unknown_event"}]
        mock_ws.return_value = self.mocking_assistant.create_websocket_mock()
        self.mocking_assistant.add_websocket_aiohttp_message(mock_ws.return_value, json.dumps(unknown_event))

        msg_queue = asyncio.Queue()
        self.listening_task = self.ev_loop.create_task(self.data_source.listen_for_user_stream(msg_queue))

        self.mocking_assistant.run_until_all_aiohttp_messages_delivered(mock_ws.return_value)

        self.assertEqual(0, msg_queue.qsize())

    # @patch("hummingbot.connector.exchange.vertex.vertex_auth.VertexAuth._time")
    # @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    # def test_listen_for_user_stream_failure_logs_error(self, ws_connect_mock, auth_time_mock):
    #     auth_time_mock.side_effect = [100]
    #     ws_connect_mock.return_value = self.mocking_assistant.create_websocket_mock()

    #     unknown_event = {"type": "unknown_event"}
    #     self.mocking_assistant.add_websocket_aiohttp_message(
    #         websocket_mock=ws_connect_mock.return_value, message=json.dumps(unknown_event)
    #     )

    #     output_queue = asyncio.Queue()

    #     self.listening_task = self.ev_loop.create_task(self.data_source.listen_for_user_stream(output=output_queue))

    #     self.mocking_assistant.run_until_all_aiohttp_messages_delivered(ws_connect_mock.return_value)

    #     sent_subscription_messages = self.mocking_assistant.json_messages_sent_through_websocket(
    #         websocket_mock=ws_connect_mock.return_value
    #     )

    #     self.assertEqual(2, len(sent_subscription_messages))
    #     self.assertTrue(self._is_logged("ERROR", "Unexpected message for user stream: {'type': 'unknown_event'}"))

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    @patch("hummingbot.core.data_type.user_stream_tracker_data_source.UserStreamTrackerDataSource._sleep")
    def test_listen_for_user_stream_iter_message_throws_exception(self, sleep_mock, mock_ws):
        msg_queue: asyncio.Queue = asyncio.Queue()
        mock_ws.return_value = self.mocking_assistant.create_websocket_mock()
        mock_ws.return_value.receive.side_effect = Exception("TEST ERROR")
        sleep_mock.side_effect = asyncio.CancelledError  # to finish the task execution

        try:
            self.async_run_with_timeout(self.data_source.listen_for_user_stream(msg_queue))
        except asyncio.CancelledError:
            pass

        self.assertTrue(
            self._is_logged("ERROR", "Unexpected error while listening to user stream. Retrying after 5 seconds...")
        )

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    @patch(
        "hummingbot.connector.derivative.vertex_perpetual.vertex_perpetual_api_user_stream_data_source.VertexPerpetualAPIUserStreamDataSource" "._time"
    )
    def test_listen_for_user_stream_subscribe_message(self, time_mock, ws_connect_mock):
        time_mock.side_effect = [1000, 1100, 1101, 1102]  # Simulate first ping interval is already due

        ws_connect_mock.return_value = self.mocking_assistant.create_websocket_mock()

        self.mocking_assistant.add_websocket_aiohttp_message(
            websocket_mock=ws_connect_mock.return_value, message=json.dumps({})
        )

        output_queue = asyncio.Queue()

        self.listening_task = self.ev_loop.create_task(self.data_source.listen_for_user_stream(output=output_queue))

        self.mocking_assistant.run_until_all_aiohttp_messages_delivered(ws_connect_mock.return_value)

        sent_messages = self.mocking_assistant.json_messages_sent_through_websocket(
            websocket_mock=ws_connect_mock.return_value
        )

        expected_message = {
            "id": 2,
            "method": "subscribe",
            "stream": {"product_id": 2, "subaccount": "0x2162Db26939B9EAF0C5404217774d166056d31B5", "type": "fill"},
        }
        self.assertEqual(expected_message, sent_messages[-2])

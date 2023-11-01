import asyncio
import sys
import time
from collections import defaultdict
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.connector.constants import s_decimal_0
from hummingbot.connector.derivative.vertex_perpetual import (
    vertex_perpetual_constants as CONSTANTS,
    vertex_perpetual_utils as utils,
    vertex_perpetual_web_utils as web_utils,
)
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.funding_info import FundingInfo, FundingInfoUpdate
from hummingbot.core.data_type.order_book import OrderBookMessage
from hummingbot.core.data_type.order_book_message import OrderBookMessageType
from hummingbot.core.data_type.perpetual_api_order_book_data_source import PerpetualAPIOrderBookDataSource
from hummingbot.core.utils.tracking_nonce import NonceCreator
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.derivative.vertex_perpetual.vertex_perpetual_derivative import VertexPerpetualDerivative


class VertexPerpetualAPIOrderBookDataSource(PerpetualAPIOrderBookDataSource):
    FULL_ORDER_BOOK_RESET_DELTA_SECONDS = sys.maxsize

    _logger: Optional[HummingbotLogger] = None

    def __init__(
        self,
        trading_pairs: List[str],
        connector: "VertexPerpetualDerivative",
        api_factory: Optional[WebAssistantsFactory] = None,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
        throttler: Optional[AsyncThrottler] = None,
    ):
        super().__init__(trading_pairs)
        self._connector = connector
        self._domain = domain
        self._throttler = throttler
        self._api_factory = api_factory or web_utils.build_api_factory(
            throttler=self._throttler,
        )
        self._message_queue: Dict[str, asyncio.Queue] = defaultdict(asyncio.Queue)
        self._last_ws_message_sent_timestamp = 0
        self._ping_interval = 0
        self._nonce_provider = NonceCreator.for_microseconds()

    def _time(self):
        return time.time()

    async def get_funding_info(self, trading_pair: str) -> FundingInfo:
        funding_info_response = await self._request_complete_funding_info(trading_pair)
        # TODO: I think we need two endpoint queries to produce what hummingbot wants here..
        # update_time = funding_info_response["update_time"]
        funding_info = FundingInfo(
            trading_pair=trading_pair,
            index_price=Decimal(str(funding_info_response["index_price"])),
            mark_price=Decimal(str(funding_info_response["mark_price"])),
            next_funding_utc_timestamp=int(funding_info_response["next_funding_utc_timestamp"]),
            rate=Decimal(str(funding_info_response["next_funding_rate"])),
        )
        return funding_info

    async def _parse_funding_info_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):

        FundingInfoUpdate(
            trading_pair=self._connector._exchange_info[raw_message["product_id"]]["market"],
            index_price=s_decimal_0,
            mark_price=s_decimal_0,
            next_funding_utc_timestamp=time.time() + 1,
            rate=s_decimal_0,
        )
        pass

    async def _parse_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        trade_timestamp: float = utils.convert_timestamp(raw_message["timestamp"])
        trade_message: OrderBookMessage = OrderBookMessage(OrderBookMessageType.TRADE, {
            "trading_pair": self._connector._exchange_info[raw_message["product_id"]]["market"],
            "trade_type": float(TradeType.BUY.value) if raw_message["is_taker_buyer"] else float(TradeType.SELL.value),
            "trade_id": int(raw_message["timestamp"]),
            "update_id": int(raw_message["timestamp"]),
            "price": utils.convert_from_x18(raw_message["price"]),
            "amount": utils.convert_from_x18(raw_message["taker_qty"])
        }, timestamp=trade_timestamp)
        message_queue.put_nowait(trade_message)

    async def _parse_order_book_diff_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        diff_timestamp = utils.convert_timestamp(raw_message["last_max_timestamp"])
        order_book_message: OrderBookMessage = OrderBookMessage(OrderBookMessageType.DIFF, {
            "trading_pair": self._connector._exchange_info[raw_message["product_id"]]["market"],
            "update_id": int(raw_message["last_max_timestamp"]),
            "bids": utils.convert_from_x18(raw_message["bids"]),
            "asks": utils.convert_from_x18(raw_message["asks"])
        }, timestamp=diff_timestamp)
        message_queue.put_nowait(order_book_message)

    async def _request_complete_funding_info(self, trading_pair: str) -> Dict[str, Any]:
        trading_pair = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        if not self._connector._exchange_info:
            await self._connector.build_exchange_info()

        product_id = utils.trading_pair_to_product_id(trading_pair=trading_pair, exchange_market_info=self._connector._exchange_info, is_perp=True)
        _funding_info_response = {}
        funding_rate_request_structure = {
            "funding_rate": {
                "product_id": product_id
            }
        }

        price_request_structure = {
            "price": {
                "product_id": product_id
            }
        }
        """
        {
        "product_id": 4,
        "funding_rate_x18": "2447900598160952",
        "update_time": "1680116326"
        }
        """
        funding_rate_data = await self._connector._api_post(
            path_url=CONSTANTS.INDEXER_PATH_URL,
            data=funding_rate_request_structure
        )
        """
        {
        "product_id": 2,
        "index_price_x18": "28180063400000000000000",
        "mark_price_x18": "28492853627394637978665",
        "update_time": "1680734493"
        }
        """
        price_data = await self._connector._api_post(
            path_url=CONSTANTS.INDEXER_PATH_URL,
            data=price_request_structure
        )
        update_time = int(funding_rate_data["update_time"])
        # update_date_time = datetime.datetime.fromtimestamp(update_time)
        # next_funding_time = update_date_time.hour + datetime.timedelta(hours=1)

        next_funding_time = int((update_time % 360) + 360)
        _funding_info_response = {
            "index_price": utils.convert_from_x18(price_data["index_price_x18"]),
            "mark_price": utils.convert_from_x18(price_data["mark_price_x18"]),
            "next_funding_utc_timestamp": next_funding_time,
            "next_funding_rate": utils.convert_from_x18(funding_rate_data["funding_rate_x18"])
        }

        return _funding_info_response

    async def get_last_traded_prices(self, trading_pairs: List[str], domain: Optional[str] = None) -> Dict[str, float]:
        return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        snapshot: Dict[str, Any] = await self._request_order_book_snapshot(trading_pair)
        snapshot_timestamp = utils.convert_timestamp(snapshot["data"]["timestamp"])
        snapshot_msg: OrderBookMessage = OrderBookMessage(OrderBookMessageType.SNAPSHOT, {
            "trading_pair": trading_pair,
            "update_id": int(snapshot_timestamp),
            "bids": utils.convert_from_x18(snapshot["data"]["bids"]),
            "asks": utils.convert_from_x18(snapshot["data"]["asks"])
        }, timestamp=snapshot_timestamp)
        return snapshot_msg

    async def _request_order_book_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        """
        Retrieves a copy of the full order book from the exchange, for a particular trading pair.

        :param trading_pair: the trading pair for which the order book will be retrieved

        :return: the response from the exchange (JSON dictionary)
        """
        if not self._connector._exchange_info:
            await self._connector.build_exchange_info()

        product_id = utils.trading_pair_to_product_id(trading_pair=trading_pair, exchange_market_info=self._connector._exchange_info, is_perp=True)

        params = {
            "type": CONSTANTS.MARKET_LIQUIDITY_REQUEST_TYPE,
            "product_id": product_id,
            "depth": CONSTANTS.ORDER_BOOK_DEPTH,
        }
        rest_assistant = await self._api_factory.get_rest_assistant()

        data = await rest_assistant.execute_request(
            url=web_utils.public_rest_url(path_url=CONSTANTS.QUERY_PATH_URL, domain=self._domain),
            params=params,
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.MARKET_LIQUIDITY_REQUEST_TYPE,
        )
        return data

    async def _subscribe_channels(self, ws: WSAssistant):
        """
        Subscribes to the trade events and diff orders events through the provided websocket connection.
        :param ws: the websocket assistant used to connect to the exchange
        """
        try:
            for trading_pair in self._trading_pairs:
                product_id = utils.trading_pair_to_product_id(trading_pair=trading_pair, exchange_market_info=self._connector._exchange_info, is_perp=True)
                trade_payload = {
                    "method": CONSTANTS.WS_SUBSCRIBE_METHOD,
                    "stream": {"type": CONSTANTS.TRADE_EVENT_TYPE, "product_id": product_id},
                    "id": product_id,
                }
                subscribe_trade_request: WSJSONRequest = WSJSONRequest(payload=trade_payload)
                order_book_payload = {
                    "method": CONSTANTS.WS_SUBSCRIBE_METHOD,
                    "stream": {"type": CONSTANTS.DIFF_EVENT_TYPE, "product_id": product_id},
                    "id": product_id,
                }
                subscribe_order_book_dif_request: WSJSONRequest = WSJSONRequest(payload=order_book_payload)

                await ws.send(subscribe_trade_request)
                await ws.send(subscribe_order_book_dif_request)

                self._last_ws_message_sent_timestamp = self._time()

                self.logger().info(f"Subscribed to public trade and order book diff channels of {trading_pair}...")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error(
                "Unexpected error occurred subscribing to trading and order book stream...", exc_info=True
            )
            raise

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        channel = ""
        if "type" in event_message:
            event_channel = event_message.get("type")
            if event_channel == CONSTANTS.TRADE_EVENT_TYPE:
                channel = self._trade_messages_queue_key
            if event_channel == CONSTANTS.DIFF_EVENT_TYPE:
                channel = self._diff_messages_queue_key

        return channel

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant):
        """
        Connects to the trade events and order diffs websocket endpoints and listens to the messages sent by the
        exchange. Each message is stored in its own queue.
        """
        while True:
            try:
                seconds_until_next_ping = self._ping_interval - (self._time() - self._last_ws_message_sent_timestamp)

                await asyncio.wait_for(super()._process_websocket_messages(websocket_assistant=websocket_assistant), timeout=seconds_until_next_ping)
            except asyncio.TimeoutError:
                ping_time = self._time()
                await websocket_assistant.ping()
                self._last_ws_message_sent_timestamp = ping_time

    async def _connected_websocket_assistant(self) -> WSAssistant:
        ws_url = f"{CONSTANTS.WSS_URLS[self._domain]}{CONSTANTS.WS_SUBSCRIBE_PATH_URL}"
        self._ping_interval = CONSTANTS.HEARTBEAT_TIME_INTERVAL
        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        await ws.connect(ws_url=ws_url, message_timeout=self._ping_interval)
        return ws

import asyncio
import json
import unittest
from collections.abc import Awaitable
from decimal import Decimal
from typing import Dict, Optional
from unittest.mock import AsyncMock, patch

from aioresponses import aioresponses
from bidict import bidict

from hummingbot.client.config.client_config_map import ClientConfigMap
from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.connector.derivative.vertex_perpetual import (
    vertex_perpetual_constants as CONSTANTS,
    vertex_perpetual_web_utils as web_utils,
)
from hummingbot.connector.derivative.vertex_perpetual.vertex_perpetual_api_order_book_data_source import (
    VertexPerpetualAPIOrderBookDataSource,
)
from hummingbot.connector.derivative.vertex_perpetual.vertex_perpetual_derivative import VertexPerpetualDerivative
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import get_new_client_order_id
from hummingbot.core.data_type.cancellation_result import CancellationResult
from hummingbot.core.data_type.common import OrderType, PositionAction, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState
from hummingbot.core.event.event_logger import EventLogger
from hummingbot.core.event.events import (
    BuyOrderCompletedEvent,
    BuyOrderCreatedEvent,
    MarketEvent,
    MarketOrderFailureEvent,
    OrderCancelledEvent,
    OrderFilledEvent,
    SellOrderCreatedEvent,
)
from hummingbot.core.network_iterator import NetworkStatus


class TestVertexPerpetualDerivative(unittest.TestCase):
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
        self.test_task: Optional[asyncio.Task] = None
        self.client_config_map = ClientConfigAdapter(ClientConfigMap())

        # NOTE: RANDOM KEYS GENERATED JUST FOR UNIT TESTS
        self.exchange = VertexPerpetualDerivative(
            client_config_map=self.client_config_map,
            vertex_perpetual_arbitrum_address="0x2162Db26939B9EAF0C5404217774d166056d31B5",  # noqa: mock
            vertex_perpetual_arbitrum_private_key="5500eb16bf3692840e04fb6a63547b9a80b75d9cbb36b43ca5662127d4c19c83",  # noqa: mock
            trading_pairs=[self.trading_pair],
            trading_required=True,
            domain=self.domain,
        )

        self.exchange.logger().setLevel(1)
        self.exchange.logger().addHandler(self)
        self.exchange._time_synchronizer.add_time_offset_ms_sample(0)
        self.exchange._time_synchronizer.logger().setLevel(1)
        self.exchange._time_synchronizer.logger().addHandler(self)
        self.exchange._order_tracker.logger().setLevel(1)
        self.exchange._order_tracker.logger().addHandler(self)

        self._initialize_event_loggers()

        VertexPerpetualAPIOrderBookDataSource._trading_pair_symbol_map = {
            CONSTANTS.DEFAULT_DOMAIN: bidict({self.ex_trading_pair: self.trading_pair})
        }

    def tearDown(self) -> None:
        self.test_task and self.test_task.cancel()
        VertexPerpetualAPIOrderBookDataSource._trading_pair_symbol_map = {}
        super().tearDown()

    def _initialize_event_loggers(self):
        self.buy_order_completed_logger = EventLogger()
        self.buy_order_created_logger = EventLogger()
        self.order_cancelled_logger = EventLogger()
        self.order_failure_logger = EventLogger()
        self.order_filled_logger = EventLogger()
        self.sell_order_completed_logger = EventLogger()
        self.sell_order_created_logger = EventLogger()

        events_and_loggers = [
            (MarketEvent.BuyOrderCompleted, self.buy_order_completed_logger),
            (MarketEvent.BuyOrderCreated, self.buy_order_created_logger),
            (MarketEvent.OrderCancelled, self.order_cancelled_logger),
            (MarketEvent.OrderFailure, self.order_failure_logger),
            (MarketEvent.OrderFilled, self.order_filled_logger),
            (MarketEvent.SellOrderCompleted, self.sell_order_completed_logger),
            (MarketEvent.SellOrderCreated, self.sell_order_created_logger),
        ]

        for event, logger in events_and_loggers:
            self.exchange.add_listener(event, logger)

    def handle(self, record):
        self.log_records.append(record)

    def _is_logged(self, log_level: str, message: str) -> bool:
        return any(record.levelname == log_level and record.getMessage() == message for record in self.log_records)

    def async_run_with_timeout(self, coroutine: Awaitable, timeout: int = 1):
        ret = self.ev_loop.run_until_complete(asyncio.wait_for(coroutine, timeout))
        return ret

    def get_query_url(self, path: str, endpoint: str) -> str:
        return f"{CONSTANTS.BASE_URLS[self.domain]}{path}?type={endpoint}"

    def get_exchange_rules_mock(self) -> Dict:
        exchange_rules = {
            "status": "success",
            "data": {
                "perp_products": [
                    {
                        "product_id": 2,
                        "oracle_price_x18": "25633794516871522686987",
                        "risk": {
                            "long_weight_initial_x18": "950000000000000000",
                            "short_weight_initial_x18": "1050000000000000000",
                            "long_weight_maintenance_x18": "970000000000000000",
                            "short_weight_maintenance_x18": "1030000000000000000",
                            "large_position_penalty_x18": "0"
                        },
                        "state": {
                            "cumulative_funding_long_x18": "6653034579736678092795",
                            "cumulative_funding_short_x18": "6653034579736678092795",
                            "available_settle": "187867449811192060288869581",
                            "open_interest": "63084001106802833576317"
                        },
                        "lp_state": {
                            "supply": "66703229552073603222444341",
                            "last_cumulative_funding_x18": "6653034579736678092795",
                            "cumulative_funding_per_lp_x18": "-298142600858196108",
                            "base": "3510756000000000000000",
                            "quote": "89948982672122498758162711"
                        },
                        "book_info": {
                            "size_increment": "1000000000000000",
                            "price_increment_x18": "1000000000000000000",
                            "min_size": "10000000000000000",
                            "collected_fees": "18816277117136627539563",
                            "lp_spread_x18": "3000000000000000"
                        }
                    },
                ]
            },
        }
        return exchange_rules

    def get_balances_mock(self) -> Dict:
        balances = {
            "status": "success",
            "data": {
                "spot_balances": [
                    {
                        "product_id": 0,
                        "lp_balance": {"amount": "0"},
                        "balance": {
                            "amount": "1000000000000000000000000",
                            "last_cumulative_multiplier_x18": "1001518877793429853",
                        },
                    },
                    {
                        "product_id": 1,
                        "lp_balance": {"amount": "0"},
                        "balance": {
                            "amount": "1000000000000000000",
                            "last_cumulative_multiplier_x18": "1001518877793429853",
                        },
                    },
                ],
                "perp_balances": [
                    {
                        "product_id": 2,
                        "lp_balance": {"amount": "0", "last_cumulative_funding_x18": "-1001518877793429853"},
                        "balance": {
                            "amount": "-100000000000000000",
                            "v_quote_balance": "100000000000000000",
                            "last_cumulative_funding_x18": "1000000000000000000",
                        },
                    }
                ],
                "spot_products": [
                    {
                        "product_id": 0,
                        "oracle_price_x18": "1000000000000000000",
                        "risk": {
                            "long_weight_initial_x18": "1000000000000000000",
                            "short_weight_initial_x18": "1000000000000000000",
                            "long_weight_maintenance_x18": "1000000000000000000",
                            "short_weight_maintenance_x18": "1000000000000000000",
                            "large_position_penalty_x18": "0",
                        },
                        "config": {
                            "token": "0x179522635726710dd7d2035a81d856de4aa7836c",
                            "interest_inflection_util_x18": "800000000000000000",
                            "interest_floor_x18": "10000000000000000",
                            "interest_small_cap_x18": "40000000000000000",
                            "interest_large_cap_x18": "1000000000000000000",
                        },
                        "state": {
                            "cumulative_deposits_multiplier_x18": "1000000008204437687",
                            "cumulative_borrows_multiplier_x18": "1003084641724797461",
                            "total_deposits_normalized": "852001296830654324383510453917856",
                            "total_borrows_normalized": "553883896490779110607466353",
                        },
                        "lp_state": {
                            "supply": "0",
                            "quote": {"amount": "0", "last_cumulative_multiplier_x18": "0"},
                            "base": {"amount": "0", "last_cumulative_multiplier_x18": "0"},
                        },
                        "book_info": {
                            "size_increment": "0",
                            "price_increment_x18": "0",
                            "min_size": "0",
                            "collected_fees": "0",
                            "lp_spread_x18": "0",
                        },
                    },
                    {
                        "product_id": 1,
                        "oracle_price_x18": "26424265624966947277660",
                        "risk": {
                            "long_weight_initial_x18": "900000000000000000",
                            "short_weight_initial_x18": "1100000000000000000",
                            "long_weight_maintenance_x18": "950000000000000000",
                            "short_weight_maintenance_x18": "1050000000000000000",
                            "large_position_penalty_x18": "0",
                        },
                        "config": {
                            "token": "0x5cc7c91690b2cbaee19a513473d73403e13fb431",
                            "interest_inflection_util_x18": "800000000000000000",
                            "interest_floor_x18": "10000000000000000",
                            "interest_small_cap_x18": "40000000000000000",
                            "interest_large_cap_x18": "1000000000000000000",
                        },
                        "state": {
                            "cumulative_deposits_multiplier_x18": "1001518877793429853",
                            "cumulative_borrows_multiplier_x18": "1005523562130424749",
                            "total_deposits_normalized": "336282930030016053702710",
                            "total_borrows_normalized": "106703872127542542861581",
                        },
                        "lp_state": {
                            "supply": "62619418496845923388438072",
                            "quote": {
                                "amount": "92286370346647961348638227",
                                "last_cumulative_multiplier_x18": "1000000008204437687",
                            },
                            "base": {
                                "amount": "3498727249394376645114",
                                "last_cumulative_multiplier_x18": "1001518877793429853",
                            },
                        },
                        "book_info": {
                            "size_increment": "1000000000000000",
                            "price_increment_x18": "1000000000000000000",
                            "min_size": "10000000000000000",
                            "collected_fees": "499223396588563365634",
                            "lp_spread_x18": "3000000000000000",
                        },
                    },
                ],
                "perp_products": [
                    {
                        "product_id": 2,
                        "oracle_price_x18": "26419259351173115090673",
                        "risk": {
                            "long_weight_initial_x18": "950000000000000000",
                            "short_weight_initial_x18": "1050000000000000000",
                            "long_weight_maintenance_x18": "970000000000000000",
                            "short_weight_maintenance_x18": "1030000000000000000",
                            "large_position_penalty_x18": "0",
                        },
                        "state": {
                            "cumulative_funding_long_x18": "6662728756561469018660",
                            "cumulative_funding_short_x18": "6662728756561469018660",
                            "available_settle": "110946828757089326230869901",
                            "open_interest": "63094032706802833576317",
                        },
                        "lp_state": {
                            "supply": "66703229552073603222444341",
                            "last_cumulative_funding_x18": "6662728756561469018660",
                            "cumulative_funding_per_lp_x18": "-298632181758973913",
                            "base": "3458209000000000000000",
                            "quote": "91332362183122498758162711",
                        },
                        "book_info": {
                            "size_increment": "1000000000000000",
                            "price_increment_x18": "1000000000000000000",
                            "min_size": "10000000000000000",
                            "collected_fees": "387217901265039386486",
                            "lp_spread_x18": "3000000000000000",
                        },
                    }
                ],
            },
        }
        return balances

    def get_matches_filled_mock(self) -> Dict:
        matches = {
            "matches": [
                {
                    "digest": "0x7b76413f438b5dd83550901304d8afed47720358acbd923890cd9431a58d3092",  # noqa: mock
                    "order": {
                        "sender": "0x2162Db26939B9EAF0C5404217774d166056d31B564656661756c740000000000",  # noqa: mock
                        "priceX18": "25000000000000000000000",
                        "amount": "1000000000000000000",
                        "expiration": "4611687704073609553",
                        "nonce": "1767528267032559689",
                    },
                    "base_filled": "1000000000000000000",
                    "quote_filled": "-250000000000000000000000000",
                    "fee": "424291087326197859",
                    "cumulative_fee": "424291087326197859",
                    "cumulative_base_filled": "1000000000000000000",
                    "cumulative_quote_filled": "-250000000000000000000000000",
                    "submission_idx": "1352436",
                }
            ],
            "txs": [
                {
                    "tx": {
                        "match_orders": {
                            "product_id": 2,
                            "amm": False,
                            "taker": {
                                "order": {
                                    "sender": "0x2162Db26939B9EAF0C5404217774d166056d31B564656661756c740000000000",  # noqa: mock
                                    "price_x18": "25000000000000000000000",
                                    "amount": "1000000000000000000",
                                    "expiration": 4611687704073609553,
                                    "nonce": 1767528267032559689,
                                },
                                "signature": "0x",
                            },
                            "maker": {
                                "order": {
                                    "sender": "0xf8d240d9514c9a4715d66268d7af3b53d619642564656661756c740000000000",  # noqa: mock
                                    "price_x18": "25000000000000000000000",
                                    "amount": "-1000000000000000000",
                                    "expiration": 1685649491,
                                    "nonce": 1767527837317726208,
                                },
                                "signature": "0x",
                            },
                        }
                    },
                    "submission_idx": "1352436",
                    "timestamp": "1685646226",
                }
            ],
        }
        return matches

    def get_matches_unfilled_mock(self) -> Dict:
        matches = {
            "matches": [
                {
                    "digest": "0x7b76413f438b5dd83550901304d8afed47720358acbd923890cd9431a58d3092",  # noqa: mock
                    "order": {
                        "sender": "0x2162Db26939B9EAF0C5404217774d166056d31B564656661756c740000000000",  # noqa: mock
                        "priceX18": "25000000000000000000000",
                        "amount": "1000000000000000000",
                        "expiration": "4611687704073609553",
                        "nonce": "1767528267032559689",
                    },
                    "base_filled": "0",
                    "quote_filled": "0",
                    "fee": "0",
                    "cumulative_fee": "0",
                    "cumulative_base_filled": "0",
                    "cumulative_quote_filled": "0",
                    "submission_idx": "1352436",
                }
            ],
            "txs": [],
        }
        return matches

    def get_order_status_mock(self) -> Dict:
        order_status = {
            "status": "success",
            "data": {
                "product_id": 2,
                "sender": "0x2162Db26939B9EAF0C5404217774d166056d31B564656661756c740000000000",  # noqa: mock
                "price_x18": "25000000000000000000000",
                "amount": "1000000000000000000",
                "expiration": "1686250284884",
                "order_type": "default",
                "nonce": "1768161672830124761",
                "unfilled_amount": "0",
                "digest": "0x7b76413f438b5dd83550901304d8afed47720358acbd923890cd9431a58d3092",  # noqa: mock
                "placed_at": 1686250288,
            },
        }
        return order_status

    def get_order_status_canceled_mock(self) -> Dict:
        order_status = {
            "status": "failure",
            "data": "Order with the provided digest (0x20ebb4ed9285ded32381c2e41258a4db33d5ffbad23c1ee609e90b37aa58f3b7) could not be found. Please verify the order digest and try again.",  # noqa: mock
            "error_code": 2020,
        }
        return order_status

    def get_partial_fill_event_mock(self) -> Dict:
        event = {
            "type": "fill",
            "timestamp": "1686256556393346680",
            "product_id": 2,
            "subaccount": "0x2162Db26939B9EAF0C5404217774d166056d31B564656661756c740000000000",  # noqa: mock
            "order_digest": "0x7b76413f438b5dd83550901304d8afed47720358acbd923890cd9431a58d3092",  # noqa: mock
            "filled_qty": "500000000000000000",
            "remaining_qty": "500000000000000000",
            "original_qty": "1000000000000000000",
            "price": "25000000000000000000000",
            "is_taker": False,
            "is_bid": True,
            "is_against_amm": False,
        }
        return event

    def get_fill_event_mock(self) -> Dict:
        event = {
            "type": "fill",
            "timestamp": "1686256556393346680",
            "product_id": 2,
            "subaccount": "0x2162Db26939B9EAF0C5404217774d166056d31B564656661756c740000000000",  # noqa: mock
            "order_digest": "0x7b76413f438b5dd83550901304d8afed47720358acbd923890cd9431a58d3092",  # noqa: mock
            "filled_qty": "1000000000000000000",
            "remaining_qty": "0",
            "original_qty": "1000000000000000000",
            "price": "25000000000000000000000",
            "is_taker": False,
            "is_bid": True,
            "is_against_amm": False,
        }
        return event

    def get_position_change_event_mock(self) -> Dict:
        event = {
            "type": "position_change",
            "timestamp": "1686256783298303728",
            "product_id": 2,
            "is_lp": False,
            "subaccount": "0x2162Db26939B9EAF0C5404217774d166056d31B564656661756c740000000000",  # noqa: mock
            "amount": "0",
            "v_quote_amount": "-10000000000000000000",
        }
        return event

    def _simulate_trading_rules_initialized(self):
        self.exchange._trading_rules = {
            self.trading_pair: TradingRule(
                trading_pair=self.trading_pair,
                min_order_size=Decimal(str(0.01)),
                min_price_increment=Decimal(str(0.0001)),
                min_base_amount_increment=Decimal(str(0.000001)),
                min_notional_size=Decimal(str(0.000001)),
                buy_order_collateral_token="USDC",
                sell_order_collateral_token="USDC",
            )
        }

    def test_supported_order_types(self):
        supported_types = self.exchange.supported_order_types()
        self.assertIn(OrderType.MARKET, supported_types)
        self.assertIn(OrderType.LIMIT, supported_types)
        self.assertIn(OrderType.LIMIT_MAKER, supported_types)

    @aioresponses()
    def test_check_network_success(self, mock_api):
        url = self.get_query_url(CONSTANTS.QUERY_PATH_URL, CONSTANTS.STATUS_REQUEST_TYPE)
        resp = {"status": "success", "data": "active"}
        mock_api.get(url, body=json.dumps(resp))

        ret = self.async_run_with_timeout(coroutine=self.exchange.check_network())

        self.assertEqual(NetworkStatus.CONNECTED, ret)

    @aioresponses()
    def test_check_network_failure(self, mock_api):
        url = self.get_query_url(CONSTANTS.QUERY_PATH_URL, CONSTANTS.STATUS_REQUEST_TYPE)
        mock_api.get(url, status=500)

        ret = self.async_run_with_timeout(coroutine=self.exchange.check_network())

        self.assertEqual(ret, NetworkStatus.NOT_CONNECTED)

    @aioresponses()
    def test_check_network_raises_cancel_exception(self, mock_api):
        url = self.get_query_url(CONSTANTS.QUERY_PATH_URL, CONSTANTS.STATUS_REQUEST_TYPE)

        mock_api.get(url, exception=asyncio.CancelledError)

        self.assertRaises(asyncio.CancelledError, self.async_run_with_timeout, self.exchange.check_network())

    @aioresponses()
    def test_update_trading_rules(self, mock_api):
        self.exchange._set_current_timestamp(1000)

        url = self.get_query_url(CONSTANTS.QUERY_PATH_URL, CONSTANTS.ALL_PRODUCTS_REQUEST_TYPE)

        resp = self.get_exchange_rules_mock()
        mock_api.get(url, body=json.dumps(resp))

        self.async_run_with_timeout(coroutine=self.exchange._update_trading_rules())

        self.assertTrue(self.trading_pair in self.exchange._trading_rules)

    def test_initial_status_dict(self):
        VertexPerpetualAPIOrderBookDataSource._trading_pair_symbol_map = {}

        status_dict = self.exchange.status_dict

        expected_initial_dict = {
            "symbols_mapping_initialized": False,
            "order_books_initialized": False,
            "account_balance": False,
            "trading_rule_initialized": False,
            "user_stream_initialized": False,
            "funding_info": False,
        }

        self.assertEqual(expected_initial_dict, status_dict)
        self.assertFalse(self.exchange.ready)

    def test_get_fee_returns_fee_from_exchange_if_available_and_default_if_not(self):
        fee = self.exchange.get_fee(
            base_currency="wBTC",
            quote_currency="USDC",
            order_type=OrderType.LIMIT,
            order_side=TradeType.BUY,
            position_action=PositionAction.OPEN,
            amount=Decimal("10"),
            price=Decimal("20"),
        )

        self.assertEqual(Decimal("0.0002"), fee.percent)  # default fee

    @patch("hummingbot.connector.utils.get_tracking_nonce")
    def test_client_order_id_on_order(self, mocked_nonce):
        mocked_nonce.return_value = 9

        result = self.exchange.buy(
            trading_pair=self.trading_pair,
            amount=Decimal("1"),
            order_type=OrderType.LIMIT,
            price=Decimal("2"),
        )
        expected_client_order_id = get_new_client_order_id(
            is_buy=True,
            trading_pair=self.trading_pair,
            hbot_order_id_prefix=CONSTANTS.HBOT_BROKER_ID,
            max_id_len=CONSTANTS.MAX_ORDER_ID_LEN,
        )

        self.assertEqual(result, expected_client_order_id)

        result = self.exchange.sell(
            trading_pair=self.trading_pair,
            amount=Decimal("1"),
            order_type=OrderType.LIMIT,
            price=Decimal("2"),
        )
        expected_client_order_id = get_new_client_order_id(
            is_buy=False,
            trading_pair=self.trading_pair,
            hbot_order_id_prefix=CONSTANTS.HBOT_BROKER_ID,
            max_id_len=CONSTANTS.MAX_ORDER_ID_LEN,
        )

        self.assertEqual(result, expected_client_order_id)

    def test_restore_tracking_states_only_registers_open_orders(self):
        orders = []
        orders.append(
            InFlightOrder(
                client_order_id="ABC1",
                exchange_order_id="EABC1",
                trading_pair=self.trading_pair,
                order_type=OrderType.LIMIT,
                trade_type=TradeType.BUY,
                amount=Decimal("1000.0"),
                price=Decimal("1.0"),
                creation_timestamp=1640001112.223,
                position=PositionAction.OPEN,
            )
        )
        orders.append(
            InFlightOrder(
                client_order_id="ABC2",
                exchange_order_id="EABC2",
                trading_pair=self.trading_pair,
                order_type=OrderType.LIMIT,
                trade_type=TradeType.BUY,
                amount=Decimal("1000.0"),
                price=Decimal("1.0"),
                creation_timestamp=1640001112.223,
                initial_state=OrderState.CANCELED,
                position=PositionAction.OPEN,
            )
        )
        orders.append(
            InFlightOrder(
                client_order_id="ABC3",
                exchange_order_id="EABC3",
                trading_pair=self.trading_pair,
                order_type=OrderType.LIMIT,
                trade_type=TradeType.BUY,
                amount=Decimal("1000.0"),
                price=Decimal("1.0"),
                creation_timestamp=1640001112.223,
                initial_state=OrderState.FILLED,
                position=PositionAction.OPEN,
            )
        )
        orders.append(
            InFlightOrder(
                client_order_id="ABC4",
                exchange_order_id="EABC4",
                trading_pair=self.trading_pair,
                order_type=OrderType.LIMIT,
                trade_type=TradeType.BUY,
                amount=Decimal("1000.0"),
                price=Decimal("1.0"),
                creation_timestamp=1640001112.223,
                initial_state=OrderState.FAILED,
                position=PositionAction.CLOSE,
            )
        )

        tracking_states = {order.client_order_id: order.to_json() for order in orders}

        self.exchange.restore_tracking_states(tracking_states)

        self.assertIn("ABC1", self.exchange.in_flight_orders)
        self.assertNotIn("ABC2", self.exchange.in_flight_orders)
        self.assertNotIn("ABC3", self.exchange.in_flight_orders)
        self.assertNotIn("ABC4", self.exchange.in_flight_orders)

    @aioresponses()
    def test_create_limit_order_successfully(self, mock_api):
        self._simulate_trading_rules_initialized()
        request_sent_event = asyncio.Event()
        self.exchange._set_current_timestamp(1640780000)
        url = web_utils.public_rest_url(CONSTANTS.POST_PATH_URL, domain=self.domain)
        creation_response = {"status": "success", "error": None}

        tradingrule_url = self.get_query_url(CONSTANTS.QUERY_PATH_URL, CONSTANTS.ALL_PRODUCTS_REQUEST_TYPE)
        resp = self.get_exchange_rules_mock()
        mock_api.get(tradingrule_url, body=json.dumps(resp))
        mock_api.post(
            url, body=json.dumps(creation_response), callback=lambda *args, **kwargs: request_sent_event.set()
        )

        self.test_task = asyncio.get_event_loop().create_task(
            self.exchange._create_order(
                trade_type=TradeType.BUY,
                order_id="ABC1",
                trading_pair=self.trading_pair,
                amount=Decimal("100"),
                order_type=OrderType.LIMIT,
                price=Decimal("10000"),
                position_action=PositionAction.OPEN,
            )
        )
        self.async_run_with_timeout(request_sent_event.wait())

        order_request = next(
            ((key, value) for key, value in mock_api.requests.items() if key[1].human_repr().startswith(url))
        )
        request_data = json.loads(order_request[1][0].kwargs["data"])["place_order"]["order"]
        self.assertEqual("0x2162Db26939B9EAF0C5404217774d166056d31B564656661756c740000000000", request_data["sender"])  # noqa: mock
        self.assertEqual("10000000000000000000000", request_data["priceX18"])
        self.assertEqual("100000000000000000000", request_data["amount"])

        self.assertIn("ABC1", self.exchange.in_flight_orders)
        create_event: BuyOrderCreatedEvent = self.buy_order_created_logger.event_log[0]
        self.assertEqual(self.exchange.current_timestamp, create_event.timestamp)
        self.assertEqual(self.trading_pair, create_event.trading_pair)
        self.assertEqual(OrderType.LIMIT, create_event.type)
        self.assertEqual(Decimal("100"), create_event.amount)
        self.assertEqual(Decimal("10000"), create_event.price)
        self.assertEqual("ABC1", create_event.order_id)

        self.assertTrue(
            self._is_logged("INFO", f"Created LIMIT BUY order ABC1 for {Decimal('100.000000')} to OPEN a {self.trading_pair} position.")
        )

    @aioresponses()
    def test_create_limit_maker_order_successfully(self, mock_api):
        self._simulate_trading_rules_initialized()
        request_sent_event = asyncio.Event()
        self.exchange._set_current_timestamp(1640780000)
        url = web_utils.public_rest_url(CONSTANTS.POST_PATH_URL, domain=self.domain)
        creation_response = {"status": "success", "error": None}

        tradingrule_url = self.get_query_url(CONSTANTS.QUERY_PATH_URL, CONSTANTS.ALL_PRODUCTS_REQUEST_TYPE)
        resp = self.get_exchange_rules_mock()
        mock_api.get(tradingrule_url, body=json.dumps(resp))
        mock_api.post(
            url, body=json.dumps(creation_response), callback=lambda *args, **kwargs: request_sent_event.set()
        )

        self.test_task = asyncio.get_event_loop().create_task(
            self.exchange._create_order(
                trade_type=TradeType.BUY,
                order_id="ABC1",
                trading_pair=self.trading_pair,
                amount=Decimal("100"),
                order_type=OrderType.LIMIT_MAKER,
                price=Decimal("10000"),
                position_action=PositionAction.OPEN,
            )
        )
        self.async_run_with_timeout(request_sent_event.wait())

        order_request = next(
            ((key, value) for key, value in mock_api.requests.items() if key[1].human_repr().startswith(url))
        )
        request_data = json.loads(order_request[1][0].kwargs["data"])["place_order"]["order"]
        self.assertEqual("0x2162Db26939B9EAF0C5404217774d166056d31B564656661756c740000000000", request_data["sender"])  # noqa: mock
        self.assertEqual("10000000000000000000000", request_data["priceX18"])
        self.assertEqual("100000000000000000000", request_data["amount"])

        self.assertIn("ABC1", self.exchange.in_flight_orders)
        create_event: BuyOrderCreatedEvent = self.buy_order_created_logger.event_log[0]
        self.assertEqual(self.exchange.current_timestamp, create_event.timestamp)
        self.assertEqual(self.trading_pair, create_event.trading_pair)
        self.assertEqual(OrderType.LIMIT_MAKER, create_event.type)
        self.assertEqual(Decimal("100"), create_event.amount)
        self.assertEqual(Decimal("10000"), create_event.price)
        self.assertEqual("ABC1", create_event.order_id)

        self.assertTrue(
            self._is_logged(
                "INFO", f"Created LIMIT_MAKER BUY order ABC1 for {Decimal('100.000000')} to OPEN a {self.trading_pair} position."
            )
        )

    @aioresponses()
    @patch("hummingbot.connector.derivative.vertex_perpetual.vertex_perpetual_derivative.VertexPerpetualDerivative.get_price")
    def test_create_market_order_successfully(self, mock_api, get_price_mock):
        get_price_mock.return_value = Decimal(1000)
        self._simulate_trading_rules_initialized()
        request_sent_event = asyncio.Event()
        self.exchange._set_current_timestamp(1640780000)
        url = web_utils.public_rest_url(CONSTANTS.POST_PATH_URL, domain=self.domain)
        creation_response = {"status": "success", "error": None}

        tradingrule_url = self.get_query_url(CONSTANTS.QUERY_PATH_URL, CONSTANTS.ALL_PRODUCTS_REQUEST_TYPE)
        resp = self.get_exchange_rules_mock()
        mock_api.get(tradingrule_url, body=json.dumps(resp))
        mock_api.post(
            url, body=json.dumps(creation_response), callback=lambda *args, **kwargs: request_sent_event.set()
        )

        self.test_task = asyncio.get_event_loop().create_task(
            self.exchange._create_order(
                trade_type=TradeType.SELL,
                order_id="ABC1",
                trading_pair=self.trading_pair,
                amount=Decimal("100"),
                order_type=OrderType.MARKET,
                price=Decimal("10000"),
                position_action=PositionAction.OPEN,
            )
        )
        self.async_run_with_timeout(request_sent_event.wait(), 10)

        order_request = next(
            ((key, value) for key, value in mock_api.requests.items() if key[1].human_repr().startswith(url))
        )
        request_data = json.loads(order_request[1][0].kwargs["data"])["place_order"]["order"]
        self.assertEqual("0x2162Db26939B9EAF0C5404217774d166056d31B564656661756c740000000000", request_data["sender"])  # noqa: mock
        self.assertEqual("10000000000000000000000", request_data["priceX18"])
        self.assertEqual("-100000000000000000000", request_data["amount"])

        self.assertIn("ABC1", self.exchange.in_flight_orders)
        create_event: SellOrderCreatedEvent = self.sell_order_created_logger.event_log[0]
        self.assertEqual(self.exchange.current_timestamp, create_event.timestamp)
        self.assertEqual(self.trading_pair, create_event.trading_pair)
        self.assertEqual(OrderType.MARKET, create_event.type)
        self.assertEqual(Decimal("100"), create_event.amount)
        self.assertEqual("ABC1", create_event.order_id)

        self.assertTrue(
            self._is_logged("INFO", f"Created MARKET SELL order ABC1 for {Decimal('100.000000')} to OPEN a {self.trading_pair} position.")
        )

    @aioresponses()
    def test_create_order_fails_and_raises_failure_event(self, mock_api):
        self._simulate_trading_rules_initialized()
        request_sent_event = asyncio.Event()
        self.exchange._set_current_timestamp(1640780000)
        url = web_utils.public_rest_url(CONSTANTS.POST_PATH_URL, domain=self.domain)
        tradingrule_url = self.get_query_url(CONSTANTS.QUERY_PATH_URL, CONSTANTS.ALL_PRODUCTS_REQUEST_TYPE)
        resp = self.get_exchange_rules_mock()
        mock_api.get(tradingrule_url, body=json.dumps(resp))
        mock_api.post(url, status=400, callback=lambda *args, **kwargs: request_sent_event.set())

        self.test_task = asyncio.get_event_loop().create_task(
            self.exchange._create_order(
                trade_type=TradeType.BUY,
                order_id="ABC1",
                trading_pair=self.trading_pair,
                amount=Decimal("100"),
                order_type=OrderType.LIMIT,
                price=Decimal("10000"),
                position_action=PositionAction.OPEN,
            )
        )
        self.async_run_with_timeout(request_sent_event.wait())

        self.assertNotIn("ABC1", self.exchange.in_flight_orders)
        self.assertEqual(0, len(self.buy_order_created_logger.event_log))
        failure_event: MarketOrderFailureEvent = self.order_failure_logger.event_log[0]
        self.assertEqual(self.exchange.current_timestamp, failure_event.timestamp)
        self.assertEqual(OrderType.LIMIT, failure_event.order_type)
        self.assertEqual("ABC1", failure_event.order_id)

        self.assertTrue(
            self._is_logged(
                "INFO",
                f"Order ABC1 has failed. Order Update: OrderUpdate(trading_pair='{self.trading_pair}', "
                f"update_timestamp={self.exchange.current_timestamp}, new_state={repr(OrderState.FAILED)}, "
                f"client_order_id='ABC1', exchange_order_id=None, misc_updates=None)",
            )
        )

    @aioresponses()
    def test_create_order_fails_when_trading_rule_error_and_raises_failure_event(self, mock_api):
        self._simulate_trading_rules_initialized()
        request_sent_event = asyncio.Event()
        self.exchange._set_current_timestamp(1640780000)

        url = web_utils.public_rest_url(CONSTANTS.POST_PATH_URL, domain=self.domain)
        tradingrule_url = self.get_query_url(CONSTANTS.QUERY_PATH_URL, CONSTANTS.ALL_PRODUCTS_REQUEST_TYPE)
        resp = self.get_exchange_rules_mock()
        mock_api.get(tradingrule_url, body=json.dumps(resp))
        mock_api.post(url, status=400, callback=lambda *args, **kwargs: request_sent_event.set())

        self.test_task = asyncio.get_event_loop().create_task(
            self.exchange._create_order(
                trade_type=TradeType.BUY,
                order_id="ABC1",
                trading_pair=self.trading_pair,
                amount=Decimal("0.0001"),
                order_type=OrderType.LIMIT,
                price=Decimal("0.0000001"),
                position_action=PositionAction.OPEN,
            )
        )
        # The second order is used only to have the event triggered and avoid using timeouts for tests
        asyncio.get_event_loop().create_task(
            self.exchange._create_order(
                trade_type=TradeType.BUY,
                order_id="ABC2",
                trading_pair=self.trading_pair,
                amount=Decimal("100"),
                order_type=OrderType.LIMIT,
                price=Decimal("10000"),
                position_action=PositionAction.CLOSE,
            )
        )

        self.async_run_with_timeout(request_sent_event.wait())

        self.assertNotIn("ABC1", self.exchange.in_flight_orders)
        self.assertAlmostEqual(0, len(self.buy_order_created_logger.event_log))
        failure_event: MarketOrderFailureEvent = self.order_failure_logger.event_log[0]
        self.assertEqual(self.exchange.current_timestamp, failure_event.timestamp)
        self.assertEqual(OrderType.LIMIT, failure_event.order_type)
        self.assertEqual("ABC1", failure_event.order_id)

        self.assertTrue(
            self._is_logged(
                "WARNING",
                "Buy order amount 0 is lower than the minimum order size 0.01. The order will not be created.",
            )
        )
        self.assertTrue(
            self._is_logged(
                "INFO",
                f"Order ABC1 has failed. Order Update: OrderUpdate(trading_pair='{self.trading_pair}', "
                f"update_timestamp={self.exchange.current_timestamp}, new_state={repr(OrderState.FAILED)}, "
                "client_order_id='ABC1', exchange_order_id=None, misc_updates=None)",
            )
        )

    @aioresponses()
    def test_cancel_order_successfully(self, mock_api):
        request_sent_event = asyncio.Event()
        self.exchange._set_current_timestamp(1640780000)

        self.exchange.start_tracking_order(
            order_id="ABC1",
            exchange_order_id="ABC1",
            trading_pair=self.trading_pair,
            trade_type=TradeType.BUY,
            price=Decimal("10000"),
            amount=Decimal("100"),
            order_type=OrderType.LIMIT,
            position_action=PositionAction.CLOSE,
        )

        self.assertIn("ABC1", self.exchange.in_flight_orders)
        order = self.exchange.in_flight_orders["ABC1"]

        url = web_utils.public_rest_url(CONSTANTS.POST_PATH_URL, domain=self.domain)
        response = {"status": "success", "error": None}

        mock_api.post(url, body=json.dumps(response), callback=lambda *args, **kwargs: request_sent_event.set())

        self.exchange.cancel(client_order_id="ABC1", trading_pair=self.trading_pair)
        self.async_run_with_timeout(request_sent_event.wait())

        cancel_event: OrderCancelledEvent = self.order_cancelled_logger.event_log[0]
        self.assertEqual(self.exchange.current_timestamp, cancel_event.timestamp)
        self.assertEqual(order.client_order_id, cancel_event.order_id)

        self.assertTrue(self._is_logged("INFO", f"Successfully canceled order {order.client_order_id}."))

    @aioresponses()
    def test_cancel_order_raises_failure_event_when_request_fails(self, mock_api):
        request_sent_event = asyncio.Event()
        self.exchange._set_current_timestamp(1640780000)

        self.exchange.start_tracking_order(
            order_id="ABC1",
            exchange_order_id="ABC1",
            trading_pair=self.trading_pair,
            trade_type=TradeType.BUY,
            price=Decimal("10000"),
            amount=Decimal("100"),
            order_type=OrderType.LIMIT,
            position_action=PositionAction.CLOSE,
        )

        self.assertIn("ABC1", self.exchange.in_flight_orders)
        order = self.exchange.in_flight_orders["ABC1"]

        url = web_utils.public_rest_url(CONSTANTS.POST_PATH_URL, domain=self.domain)

        mock_api.post(url, status=400, callback=lambda *args, **kwargs: request_sent_event.set())

        self.exchange.cancel(client_order_id="ABC1", trading_pair=self.trading_pair)
        self.async_run_with_timeout(request_sent_event.wait())

        self.assertAlmostEqual(0, len(self.order_cancelled_logger.event_log))

        self.assertTrue(self._is_logged("ERROR", f"Failed to cancel order {order.client_order_id}"))

    @aioresponses()
    def test_cancel_two_orders_with_cancel_all_and_one_fails(self, mock_api):
        self.exchange._set_current_timestamp(1640780000)

        self.exchange.start_tracking_order(
            order_id="ABC1",
            exchange_order_id="ABC1",
            trading_pair=self.trading_pair,
            trade_type=TradeType.BUY,
            price=Decimal("10000"),
            amount=Decimal("100"),
            order_type=OrderType.LIMIT,
            position_action=PositionAction.CLOSE,
        )

        self.assertIn("ABC1", self.exchange.in_flight_orders)
        order1 = self.exchange.in_flight_orders["ABC1"]

        self.exchange.start_tracking_order(
            order_id="ABC2",
            exchange_order_id="ABC2",
            trading_pair=self.trading_pair,
            trade_type=TradeType.SELL,
            price=Decimal("11000"),
            amount=Decimal("90"),
            order_type=OrderType.LIMIT,
            position_action=PositionAction.CLOSE,
        )

        self.assertIn("ABC2", self.exchange.in_flight_orders)
        order2 = self.exchange.in_flight_orders["ABC2"]

        url = web_utils.public_rest_url(CONSTANTS.POST_PATH_URL, domain=self.domain)

        response = {"status": "success", "error": None}

        mock_api.post(url, body=json.dumps(response))

        url = web_utils.public_rest_url(CONSTANTS.POST_PATH_URL, domain=self.domain)

        mock_api.post(url, status=400)

        cancellation_results = self.async_run_with_timeout(self.exchange.cancel_all(10))

        self.assertEqual(2, len(cancellation_results))
        self.assertEqual(CancellationResult(order1.client_order_id, True), cancellation_results[0])
        self.assertEqual(CancellationResult(order2.client_order_id, False), cancellation_results[1])

        self.assertAlmostEqual(1, len(self.order_cancelled_logger.event_log))
        cancel_event: OrderCancelledEvent = self.order_cancelled_logger.event_log[0]
        self.assertEqual(self.exchange.current_timestamp, cancel_event.timestamp)
        self.assertEqual(order1.client_order_id, cancel_event.order_id)

        self.assertTrue(self._is_logged("INFO", f"Successfully canceled order {order1.client_order_id}."))

    @aioresponses()
    @patch("hummingbot.connector.time_synchronizer.TimeSynchronizer._current_seconds_counter")
    def test_update_time_synchronizer_successfully(self, mock_api, seconds_counter_mock):
        seconds_counter_mock.side_effect = [0, 0, 0]
        self.exchange._set_current_timestamp(1640780000)

        self.exchange._time_synchronizer.clear_time_offset_ms_samples()
        url = self.get_query_url(CONSTANTS.QUERY_PATH_URL, CONSTANTS.STATUS_REQUEST_TYPE)

        response = {"status": "success", "data": "active"}

        mock_api.get(url, body=json.dumps(response))

        self.async_run_with_timeout(self.exchange._update_time_synchronizer())
        self.assertLess(1640780000 * 1e-3, self.exchange._time_synchronizer.time())

    @aioresponses()
    def test_update_time_synchronizer_failure_is_logged(self, mock_api):
        self.exchange._set_current_timestamp(1640780000)
        url = self.get_query_url(CONSTANTS.QUERY_PATH_URL, CONSTANTS.STATUS_REQUEST_TYPE)

        response = {"status": "success", "data": "failed"}

        mock_api.get(url, body=json.dumps(response), status=400)

        self.async_run_with_timeout(self.exchange._update_time_synchronizer())
        self.assertGreater(1640780000 * 1e-3, self.exchange._time_synchronizer.time())

    @aioresponses()
    def test_update_balances(self, mock_api):
        url = f"{CONSTANTS.BASE_URLS[self.domain]}/query?subaccount={self.exchange.sender_address}&type=subaccount_info"
        response = self.get_balances_mock()

        mock_api.get(url, body=json.dumps(response))
        self.async_run_with_timeout(self.exchange._update_balances())

        available_balances = self.exchange.available_balances
        total_balances = self.exchange.get_all_balances()

        # self.assertEqual(Decimal("1"), available_balances["wBTC"])
        self.assertEqual(Decimal("1023782"), available_balances["USDC"].quantize(Decimal("1")))
        # self.assertEqual(Decimal("1"), total_balances["wBTC"])
        self.assertEqual(Decimal("1026424.26562496694727766"), total_balances["USDC"])

    @aioresponses()
    def test_update_order_status_when_filled(self, mock_api):
        self.exchange._set_current_timestamp(1640780000)
        self.exchange._last_poll_timestamp = self.exchange.current_timestamp - 10 - 1
        digest = "0x7b76413f438b5dd83550901304d8afed47720358acbd923890cd9431a58d3092"  # noqa: mock

        self.exchange.start_tracking_order(
            order_id=digest,
            exchange_order_id=digest,
            trading_pair=self.trading_pair,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("25000"),
            amount=Decimal("1"),
            position_action=PositionAction.OPEN,
        )
        order: InFlightOrder = self.exchange.in_flight_orders[digest]

        matches_url = web_utils.public_rest_url(CONSTANTS.INDEXER_PATH_URL, domain=self.domain)
        matches_response = self.get_matches_filled_mock()
        mock_api.post(matches_url, body=json.dumps(matches_response))

        orders_url = f"{CONSTANTS.BASE_URLS[self.domain]}/query?digest={digest}&product_id=2&type=order"
        orders_response = self.get_order_status_mock()
        mock_api.get(orders_url, body=json.dumps(orders_response))

        # Simulate the order has been filled with a TradeUpdate
        order.completely_filled_event.set()
        self.async_run_with_timeout(self.exchange._update_order_status())
        self.async_run_with_timeout(order.wait_until_completely_filled())

        self.assertTrue(order.is_filled)
        self.assertTrue(order.is_done)

        buy_event: BuyOrderCompletedEvent = self.buy_order_completed_logger.event_log[0]
        self.assertEqual(self.exchange.current_timestamp, buy_event.timestamp)
        self.assertEqual(order.client_order_id, buy_event.order_id)
        self.assertEqual(order.base_asset, buy_event.base_asset)
        self.assertEqual(order.quote_asset, buy_event.quote_asset)
        self.assertEqual(Decimal("1"), buy_event.base_asset_amount)
        self.assertEqual(Decimal("25000"), buy_event.quote_asset_amount)
        self.assertEqual(order.order_type, buy_event.order_type)
        self.assertEqual(order.exchange_order_id, buy_event.exchange_order_id)
        self.assertNotIn(order.client_order_id, self.exchange.in_flight_orders)
        self.assertTrue(self._is_logged("INFO", f"BUY order {order.client_order_id} completely filled."))

    @aioresponses()
    def test_update_order_status_when_cancelled(self, mock_api):
        self.exchange._set_current_timestamp(1640780000)
        self.exchange._last_poll_timestamp = self.exchange.current_timestamp - 10 - 1
        digest = "0x7b76413f438b5dd83550901304d8afed47720358acbd923890cd9431a58d3092"  # noqa: mock

        self.exchange.start_tracking_order(
            order_id=digest,
            exchange_order_id=digest,
            trading_pair=self.trading_pair,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("25000"),
            amount=Decimal("1"),
            position_action=PositionAction.CLOSE,
        )
        order: InFlightOrder = self.exchange.in_flight_orders[digest]

        matches_url = web_utils.public_rest_url(CONSTANTS.INDEXER_PATH_URL, domain=self.domain)
        matches_response = self.get_matches_unfilled_mock()
        mock_api.post(matches_url, body=json.dumps(matches_response))

        orders_url = f"{CONSTANTS.BASE_URLS[self.domain]}/query?digest={digest}&product_id=2&type=order"
        orders_response = self.get_order_status_canceled_mock()
        mock_api.get(orders_url, body=json.dumps(orders_response))

        self.async_run_with_timeout(self.exchange._update_order_status())

        cancel_event: OrderCancelledEvent = self.order_cancelled_logger.event_log[0]
        self.assertEqual(self.exchange.current_timestamp, cancel_event.timestamp)
        self.assertEqual(order.client_order_id, cancel_event.order_id)
        self.assertEqual(order.exchange_order_id, cancel_event.exchange_order_id)
        self.assertNotIn(order.client_order_id, self.exchange.in_flight_orders)
        self.assertTrue(self._is_logged("INFO", f"Successfully canceled order {order.client_order_id}."))

    @aioresponses()
    def test_update_order_status_when_order_has_not_changed(self, mock_api):
        self.exchange._set_current_timestamp(1640780000)
        self.exchange._last_poll_timestamp = self.exchange.current_timestamp - 10 - 1
        digest = "0x7b76413f438b5dd83550901304d8afed47720358acbd923890cd9431a58d3092"  # noqa: mock

        self.exchange.start_tracking_order(
            order_id=digest,
            exchange_order_id=digest,
            trading_pair=self.trading_pair,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("25000"),
            amount=Decimal("1"),
            position_action=PositionAction.OPEN,
        )
        order: InFlightOrder = self.exchange.in_flight_orders[digest]

        matches_url = web_utils.public_rest_url(CONSTANTS.INDEXER_PATH_URL, domain=self.domain)
        matches_response = self.get_matches_unfilled_mock()
        mock_api.post(matches_url, body=json.dumps(matches_response))

        orders_url = f"{CONSTANTS.BASE_URLS[self.domain]}/query?digest={digest}&product_id=2&type=order"
        orders_response = self.get_order_status_mock()
        mock_api.get(orders_url, body=json.dumps(orders_response))

        self.assertTrue(order.is_open)

        self.async_run_with_timeout(self.exchange._update_order_status())

        self.assertTrue(order.is_open)
        self.assertFalse(order.is_filled)
        self.assertFalse(order.is_done)

    @aioresponses()
    def test_update_order_status_when_request_fails_marks_order_as_not_found(self, mock_api):
        self.exchange._set_current_timestamp(1640780000)
        self.exchange._last_poll_timestamp = self.exchange.current_timestamp - 10 - 1
        digest = "0x7b76413f438b5dd83550901304d8afed47720358acbd923890cd9431a58d3092"  # noqa: mock

        self.exchange.start_tracking_order(
            order_id=digest,
            exchange_order_id=digest,
            trading_pair=self.trading_pair,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("25000"),
            amount=Decimal("1"),
            position_action=PositionAction.CLOSE,
        )
        order: InFlightOrder = self.exchange.in_flight_orders[digest]

        orders_url = f"{CONSTANTS.BASE_URLS[self.domain]}/query?digest={digest}&product_id=2&type=order"
        mock_api.get(orders_url, status=404)

        self.async_run_with_timeout(self.exchange._update_order_status())

        self.assertTrue(order.is_open)
        self.assertFalse(order.is_filled)
        self.assertFalse(order.is_done)

        self.assertEqual(1, self.exchange._order_tracker._order_not_found_records[order.client_order_id])

    @aioresponses()
    def test_user_stream_update_for_new_order_does_not_update_status(self, mock_api):
        self.exchange._set_current_timestamp(1640780000)
        digest = "0x7b76413f438b5dd83550901304d8afed47720358acbd923890cd9431a58d3092"  # noqa: mock

        self.exchange.start_tracking_order(
            order_id=digest,
            exchange_order_id=digest,
            trading_pair=self.trading_pair,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("25000"),
            amount=Decimal("1"),
            position_action=PositionAction.CLOSE,
        )
        order: InFlightOrder = self.exchange.in_flight_orders[digest]

        matches_url = web_utils.public_rest_url(CONSTANTS.INDEXER_PATH_URL, domain=self.domain)
        matches_response = self.get_matches_unfilled_mock()
        mock_api.post(matches_url, body=json.dumps(matches_response))

        orders_url = f"{CONSTANTS.BASE_URLS[self.domain]}/query?digest={digest}&product_id=2&type=order"
        orders_response = self.get_order_status_mock()
        mock_api.get(orders_url, body=json.dumps(orders_response))
        self.async_run_with_timeout(self.exchange._update_order_status())

        event_message = {"type": "nonexistent_vertex_event"}
        mock_queue = AsyncMock()
        mock_queue.get.side_effect = [event_message, asyncio.CancelledError]
        self.exchange._user_stream_tracker._user_stream = mock_queue

        try:
            self.async_run_with_timeout(self.exchange._user_stream_event_listener())
        except asyncio.CancelledError:
            pass

        self.assertTrue(order.is_open)

        self.async_run_with_timeout(self.exchange._update_order_status())

        self.assertTrue(order.is_open)
        self.assertFalse(order.is_filled)
        self.assertFalse(order.is_done)

    @aioresponses()
    def test_user_stream_update_for_cancelled_order(self, mock_api):
        self.exchange._set_current_timestamp(1640780000)
        digest = "0x7b76413f438b5dd83550901304d8afed47720358acbd923890cd9431a58d3092"  # noqa: mock

        self.exchange.start_tracking_order(
            order_id=digest,
            exchange_order_id=digest,
            trading_pair=self.trading_pair,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("25000"),
            amount=Decimal("1"),
            position_action=PositionAction.CLOSE,
        )
        order: InFlightOrder = self.exchange.in_flight_orders[digest]

        matches_url = web_utils.public_rest_url(CONSTANTS.INDEXER_PATH_URL, domain=self.domain)
        matches_response = self.get_matches_unfilled_mock()
        mock_api.post(matches_url, body=json.dumps(matches_response))

        orders_url = f"{CONSTANTS.BASE_URLS[self.domain]}/query?digest={digest}&product_id=2&type=order"
        orders_response = self.get_order_status_canceled_mock()
        mock_api.get(orders_url, body=json.dumps(orders_response))

        self.async_run_with_timeout(self.exchange._update_order_status())

        event_message = {"type": "nonexistent_vertex_event"}
        mock_queue = AsyncMock()
        mock_queue.get.side_effect = [event_message, asyncio.CancelledError]
        self.exchange._user_stream_tracker._user_stream = mock_queue

        try:
            self.async_run_with_timeout(self.exchange._user_stream_event_listener())
        except asyncio.CancelledError:
            pass

        cancel_event: OrderCancelledEvent = self.order_cancelled_logger.event_log[0]
        self.assertEqual(self.exchange.current_timestamp, cancel_event.timestamp)
        self.assertEqual(order.client_order_id, cancel_event.order_id)
        self.assertEqual(order.exchange_order_id, cancel_event.exchange_order_id)
        self.assertNotIn(order.client_order_id, self.exchange.in_flight_orders)
        self.assertTrue(order.is_cancelled)
        self.assertTrue(order.is_done)

        self.assertTrue(self._is_logged("INFO", f"Successfully canceled order {order.client_order_id}."))

    def test_user_stream_update_for_order_partial_fill(self):
        self.exchange._set_current_timestamp(1640780000)
        digest = "0x7b76413f438b5dd83550901304d8afed47720358acbd923890cd9431a58d3092"  # noqa: mock

        self.exchange.start_tracking_order(
            order_id=digest,
            exchange_order_id=digest,
            trading_pair=self.trading_pair,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("25000"),
            amount=Decimal("1"),
            position_action=PositionAction.CLOSE,
        )
        order: InFlightOrder = self.exchange.in_flight_orders[digest]

        event_message = self.get_partial_fill_event_mock()

        mock_queue = AsyncMock()
        mock_queue.get.side_effect = [event_message, asyncio.CancelledError]
        self.exchange._user_stream_tracker._user_stream = mock_queue

        try:
            self.async_run_with_timeout(self.exchange._user_stream_event_listener())
        except asyncio.CancelledError:
            pass

        self.assertTrue(order.is_open)
        self.assertEqual(OrderState.PARTIALLY_FILLED, order.current_state)

        fill_event: OrderFilledEvent = self.order_filled_logger.event_log[0]
        self.assertEqual(self.exchange.current_timestamp, fill_event.timestamp)
        self.assertEqual(order.client_order_id, fill_event.order_id)
        self.assertEqual(order.trading_pair, fill_event.trading_pair)
        self.assertEqual(order.trade_type, fill_event.trade_type)
        self.assertEqual(order.order_type, fill_event.order_type)
        self.assertEqual(Decimal("25000"), fill_event.price)
        self.assertEqual(Decimal("0.5"), fill_event.amount)

        self.assertEqual(0, len(self.buy_order_completed_logger.event_log))

        self.assertTrue(
            self._is_logged(
                "INFO",
                f"The {order.trade_type.name} order {order.client_order_id} amounting to "
                f"{fill_event.amount}/{order.amount} {order.base_asset} has been filled.",
            )
        )

    def test_user_stream_update_for_order_fill(self):
        self.exchange._set_current_timestamp(1640780000)
        digest = "0x7b76413f438b5dd83550901304d8afed47720358acbd923890cd9431a58d3092"  # noqa: mock

        self.exchange.start_tracking_order(
            order_id=digest,
            exchange_order_id=digest,
            trading_pair=self.trading_pair,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("25000"),
            amount=Decimal("1"),
            position_action=PositionAction.OPEN,
        )
        order: InFlightOrder = self.exchange.in_flight_orders[digest]

        event_message = self.get_fill_event_mock()

        mock_queue = AsyncMock()
        mock_queue.get.side_effect = [event_message, asyncio.CancelledError]
        self.exchange._user_stream_tracker._user_stream = mock_queue

        try:
            self.async_run_with_timeout(self.exchange._user_stream_event_listener())
        except asyncio.CancelledError:
            pass

        fill_event: OrderFilledEvent = self.order_filled_logger.event_log[0]
        self.assertEqual(self.exchange.current_timestamp, fill_event.timestamp)
        self.assertEqual(order.client_order_id, fill_event.order_id)
        self.assertEqual(order.trading_pair, fill_event.trading_pair)
        self.assertEqual(order.trade_type, fill_event.trade_type)
        self.assertEqual(order.order_type, fill_event.order_type)
        self.assertEqual(Decimal("25000"), fill_event.price)
        self.assertEqual(Decimal("1"), fill_event.amount)

        buy_event: BuyOrderCompletedEvent = self.buy_order_completed_logger.event_log[0]
        self.assertEqual(self.exchange.current_timestamp, buy_event.timestamp)
        self.assertEqual(order.client_order_id, buy_event.order_id)
        self.assertEqual(order.base_asset, buy_event.base_asset)
        self.assertEqual(order.quote_asset, buy_event.quote_asset)
        self.assertEqual(order.amount, buy_event.base_asset_amount)
        self.assertEqual(Decimal("25000"), buy_event.quote_asset_amount)
        self.assertEqual(order.order_type, buy_event.order_type)
        self.assertEqual(order.exchange_order_id, buy_event.exchange_order_id)
        self.assertNotIn(order.client_order_id, self.exchange.in_flight_orders)
        self.assertTrue(order.is_filled)
        self.assertTrue(order.is_done)

        self.assertTrue(self._is_logged("INFO", f"BUY order {order.client_order_id} completely filled."))

    def test_user_stream_balance_update(self):
        self.exchange._set_current_timestamp(1640780000)

        event_message = self.get_position_change_event_mock()
        mock_queue = AsyncMock()
        mock_queue.get.side_effect = [event_message, asyncio.CancelledError]
        self.exchange._user_stream_tracker._user_stream = mock_queue

        try:
            self.async_run_with_timeout(self.exchange._user_stream_event_listener())
        except asyncio.CancelledError:
            pass

        self.assertEqual(Decimal("-10"), self.exchange.available_balances["USDC"])
        self.assertEqual(Decimal("-10"), self.exchange.get_balance("USDC"))

    def test_user_stream_raises_cancel_exception(self):
        self.exchange._set_current_timestamp(1640780000)

        mock_queue = AsyncMock()
        mock_queue.get.side_effect = asyncio.CancelledError
        self.exchange._user_stream_tracker._user_stream = mock_queue

        self.assertRaises(
            asyncio.CancelledError, self.async_run_with_timeout, self.exchange._user_stream_event_listener()
        )

    @patch("hummingbot.connector.derivative.vertex_perpetual.vertex_perpetual_derivative.VertexPerpetualDerivative._sleep")
    def test_user_stream_logs_errors(self, _):
        self.exchange._set_current_timestamp(1640780000)

        incomplete_event = {"type": "incomplete_event"}

        mock_queue = AsyncMock()
        mock_queue.get.side_effect = [incomplete_event, asyncio.CancelledError]
        self.exchange._user_stream_tracker._user_stream = mock_queue

        try:
            self.async_run_with_timeout(self.exchange._user_stream_event_listener())
        except asyncio.CancelledError:
            pass

        self.assertTrue(
            self._is_logged("ERROR", f"Unexpected event `{incomplete_event['type']}` in user stream listener loop.")
        )

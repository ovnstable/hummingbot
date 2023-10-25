import asyncio
import time
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from bidict import bidict

import hummingbot.connector.derivative.vertex_perpetual.vertex_perpetual_constants as CONSTANTS
import hummingbot.connector.derivative.vertex_perpetual.vertex_perpetual_eip712_structs as vertex_perpetual_eip712_structs
import hummingbot.connector.derivative.vertex_perpetual.vertex_perpetual_utils as utils
from hummingbot.connector.constants import s_decimal_0, s_decimal_NaN
from hummingbot.connector.derivative.position import Position
from hummingbot.connector.derivative.vertex_perpetual import vertex_perpetual_web_utils as web_utils
from hummingbot.connector.derivative.vertex_perpetual.vertex_perpetual_api_order_book_data_source import (
    VertexPerpetualAPIOrderBookDataSource,
)
from hummingbot.connector.derivative.vertex_perpetual.vertex_perpetual_api_user_stream_data_source import (
    VertexPerpetualAPIUserStreamDataSource,
)
from hummingbot.connector.derivative.vertex_perpetual.vertex_perpetual_auth import VertexPerpetualAuth
from hummingbot.connector.perpetual_derivative_py_base import PerpetualDerivativePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import combine_to_hb_trading_pair
from hummingbot.core.data_type.common import OrderType, PositionAction, PositionMode, PositionSide, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, TokenAmount, TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.utils.async_utils import safe_gather
from hummingbot.core.utils.estimate_fee import build_perpetual_trade_fee
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

if TYPE_CHECKING:
    from hummingbot.client.config.config_helpers import ClientConfigAdapter


class VertexPerpetualDerivative(PerpetualDerivativePyBase):
    web_utils = web_utils

    def __init__(
        self,
        client_config_map: "ClientConfigAdapter",
        vertex_perpetual_arbitrum_address: str,
        vertex_perpetual_arbitrum_private_key: str,
        trading_pairs: Optional[List[str]] = None,
        trading_required: bool = True,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ):
        self.public_key = vertex_perpetual_arbitrum_address
        self.sender_address = utils.convert_address_to_sender(vertex_perpetual_arbitrum_address)
        self.private_key = vertex_perpetual_arbitrum_private_key
        self._trading_pairs = trading_pairs
        self._trading_required = trading_required
        self._domain = domain
        self.real_time_balance_update = False
        self._position_mode = None
        self._order_notional_amounts = {}
        self._current_place_order_requests = 0
        self._rate_limits_config = {}
        self._margin_fractions = {}
        self._position_id = None
        self._exchange_info = {}
        self._use_spot_leverage = True

        self._allocated_collateral = {}
        self._allocated_collateral_sum = s_decimal_0
        self._last_trades_poll_vertex_perpetual_timestamp = 1.0
        super().__init__(client_config_map=client_config_map)

    @staticmethod
    def vertex_order_type(order_type: OrderType) -> str:
        return order_type.name.upper()

    @staticmethod
    def to_hb_order_type(vertex_type: str) -> OrderType:
        return OrderType[vertex_type]

    @property
    def name(self) -> str:
        if self._domain == "vertex_perpetual":
            return "vertex_perpetual"
        return "vertex_perpetual_testnet"

    @property
    def authenticator(self):
        return VertexPerpetualAuth(
            vertex_perpetual_arbitrum_address=self.sender_address,
            vertex_perpetual_arbitrum_private_key=self.private_key
        )

    @property
    def domain(self) -> str:
        return self._domain

    @property
    def rate_limits_rules(self):
        return CONSTANTS.RATE_LIMITS

    @property
    def client_order_id_max_length(self) -> int:
        return CONSTANTS.MAX_ORDER_ID_LEN

    @property
    def client_order_id_prefix(self) -> str:
        return CONSTANTS.HBOT_BROKER_ID

    @property
    def trading_rules_request_path(self):
        return CONSTANTS.QUERY_PATH_URL + "?type=" + CONSTANTS.ALL_PRODUCTS_REQUEST_TYPE

    @property
    def trading_pairs_request_path(self):
        return CONSTANTS.QUERY_PATH_URL + "?type=" + CONSTANTS.ALL_PRODUCTS_REQUEST_TYPE

    @property
    def check_network_request_path(self):
        return CONSTANTS.QUERY_PATH_URL + "?type=" + CONSTANTS.STATUS_REQUEST_TYPE

    @property
    def trading_pairs(self) -> List[str]:
        return self._trading_pairs

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        return True

    @property
    def is_trading_required(self) -> bool:
        return self._trading_required

    @property
    def funding_fee_poll_interval(self) -> int:
        return 320

    def supported_position_modes(self):
        """
        This method needs to be overridden to provide the accurate information depending on the exchange.
        """
        return [PositionMode.ONEWAY]  # pragma no cover

    def get_buy_collateral_token(self, trading_pair: str) -> str:
        trading_rule: TradingRule = self._trading_rules[trading_pair]
        return trading_rule.buy_order_collateral_token

    def get_sell_collateral_token(self, trading_pair: str) -> str:
        trading_rule: TradingRule = self._trading_rules[trading_pair]
        return trading_rule.sell_order_collateral_token

    def supported_order_types(self):
        # https://vertex-protocol.gitbook.io/docs/developer-resources/api/websocket-rest-api/executes/place-order#signing
        """
        0 ⇒ Default order, where it will attempt to take from the book and then become a resting limit order if there is quantity remaining
        1 ⇒ Immediate-or-cancel order, which is the same as a default order except it doesn’t become a resting limit order
        2 ⇒ Fill-or-kill order, which is the same as an IOC order except either the entire order has to be filled or none of it.
        3 ⇒ Post-only order, where the order is not allowed to take from the book. An error is returned if the order would cross the bid ask spread.
        """
        return [OrderType.MARKET, OrderType.LIMIT, OrderType.LIMIT_MAKER]

    async def start_network(self):
        await super().start_network()
        await self.build_exchange_info()
        await self._update_trading_rules()
        await self._update_trading_fees()

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception) -> bool:
        # TODO: implement this method correctly for the connector
        # The default implementation was added when the functionality to detect not found orders was introduced in the
        # ExchangePyBase class. Also fix the unit test test_lost_order_removed_if_not_found_during_order_status_update
        # when replacing the dummy implementation
        return False

    def _is_request_result_an_error_related_to_time_synchronizer(self, request_result: Dict[str, Any]) -> bool:
        # TODO: implement this method correctly for the connector
        # The default implementation was added when the functionality to detect not found orders was introduced in the
        # ExchangePyBase class. Also fix the unit test test_lost_order_removed_if_not_found_during_order_status_update
        # when replacing the dummy implementation
        return False

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        # TODO: implement this method correctly for the connector
        # The default implementation was added when the functionality to detect not found orders was introduced in the
        # ExchangePyBase class. Also fix the unit test test_lost_order_removed_if_not_found_during_order_status_update
        # when replacing the dummy implementation
        return False

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        # TODO: implement this method correctly for the connector
        # The default implementation was added when the functionality to detect not found orders was introduced in the
        # ExchangePyBase class. Also fix the unit test test_cancel_order_not_found_in_the_exchange when replacing the
        # dummy implementation
        return False

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return web_utils.build_api_factory(
            throttler=self._throttler,
            auth=self._auth)

    def _create_order_book_data_source(self) -> VertexPerpetualAPIOrderBookDataSource:
        return VertexPerpetualAPIOrderBookDataSource(
            self._trading_pairs,
            connector=self,
            domain=self.domain,
            api_factory=self._web_assistants_factory,
        )

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return VertexPerpetualAPIUserStreamDataSource(
            trading_pairs=self._trading_pairs,
            api_factory=self._web_assistants_factory,
            connector=self,
            auth=self.authenticator,
            domain=self.domain,
        )

    async def _status_polling_loop_fetch_updates(self):
        await safe_gather(
            self._update_order_status(),
            self._update_balances(),
            self._update_trading_fees(),
            self._update_positions(),
        )

    async def _get_position_mode(self) -> Optional[PositionMode]:
        # NOTE: This is default to ONEWAY as there is nothing available on current version of Vega
        return self._position_mode

    async def _trading_pair_position_mode_set(self, mode: PositionMode, trading_pair: str) -> Tuple[bool, str]:
        """
        :return: A tuple of boolean (true if success) and error message if the exchange returns one on failure.
        """
        msg = ""
        return True, msg

    async def _set_trading_pair_leverage(self, trading_pair: str, leverage: int) -> Tuple[bool, str]:
        # TODO: This is a concept which the UI provides, but the order doesn't care. So it's a calculation
        # done somewhere to make this work...
        # long_weight_initial_x18
        # short_weight_initial_x18
        success = True
        msg = ""

        # TODO: Review
        # max_leverage = int(Decimal("1") / self._margin_fractions[trading_pair]["initial"])
        leverage = int(Decimal("1") / Decimal("0.5"))
        max_leverage = int(Decimal("1") / Decimal("0.5"))
        if leverage > max_leverage:
            self._perpetual_trading.set_leverage(trading_pair=trading_pair, leverage=max_leverage)
            self.logger().warning(f"Leverage has been reduced to {max_leverage}")
        else:
            self._perpetual_trading.set_leverage(trading_pair=trading_pair, leverage=leverage)
        return success, msg

    async def _fetch_last_fee_payment(self, trading_pair: str) -> Tuple[int, Decimal, Decimal]:
        """
        Returns a tuple of the latest funding payment timestamp, funding rate, and payment amount.
        If no payment exists, return (0, -1, -1)
        """
        sender_address = self.sender_address
        product_id = utils.trading_pair_to_product_id(trading_pair=trading_pair, exchange_market_info=self._exchange_info, is_perp=True)
        data = {
            "events": {
                "product_ids": [product_id],
                "subaccount": sender_address,
                # TODO: For perps do we care about match orders?
                "event_types": ["match_orders", "settle_pnl"],
                # "max_time": 1679728762,
                # "limit": {
                #     "raw": 500
                # }
            }
        }
        timestamp, funding_rate, payment = 0, Decimal("-1"), Decimal("-1")

        if product_id == -1:
            return timestamp, funding_rate, payment

        response: Dict[str, Any] = await self._api_post(
            path_url=CONSTANTS.INDEXER_PATH_URL,
            data=data,
            limit_id=CONSTANTS.INDEXER_PATH_URL
        )

        if "events" not in response:
            self.logger().warning(f"Unable to get payment update summaries for subaccount {sender_address}")
            return timestamp, funding_rate, payment

        last_payment_balance: Optional[Decimal] = None
        previous_payment_balance: Optional[Decimal] = None
        submission_idx: Optional[str] = None
        for products in response["events"]:
            if product_id == products["product_id"]:
                if last_payment_balance is None:
                    last_payment_balance = Decimal(utils.convert_from_x18(products["net_funding_cumulative"]))
                    submission_idx = products["submission_idx"]
                else:
                    previous_payment_balance = Decimal(utils.convert_from_x18(products["net_funding_cumulative"]))
                if previous_payment_balance is not None:
                    break

        for transaction in response["txs"]:
            if "match_orders" in transaction["tx"]:
                _transaction = transaction["tx"]["match_orders"]
                _parsed_product_id = _transaction["product_id"]
            if "settle_pnl" in transaction["tx"]:
                _transaction = transaction["tx"]["settle_pnl"]
                _parsed_product_id = int(_transaction["product_ids"][0].replace("0x", ""))
            if product_id == _parsed_product_id:
                if submission_idx is not None and transaction["submission_idx"] == submission_idx:
                    timestamp = float(transaction["timestamp"])
                    break
        if (last_payment_balance is not None) and (previous_payment_balance is not None):
            payment = last_payment_balance - previous_payment_balance

        rate = await self._get_funding_rate(product_id=product_id)
        funding_rate = Decimal(utils.convert_from_x18(rate["funding_rate_x18"]))

        return timestamp, funding_rate, payment

    def _get_fee(
        self,
        base_currency: str,
        quote_currency: str,
        order_type: OrderType,
        order_side: TradeType,
        position_action: PositionAction,
        amount: Decimal,
        price: Decimal = s_decimal_NaN,
        is_maker: Optional[bool] = None,
    ) -> TradeFeeBase:
        trading_pair = f"{base_currency}-{quote_currency}"
        is_maker = is_maker or False
        if trading_pair not in self._trading_fees:
            fee = build_perpetual_trade_fee(
                self.name,
                is_maker,
                position_action=position_action,
                base_currency=base_currency,
                quote_currency=quote_currency,
                order_type=order_type,
                order_side=order_side,
                amount=amount,
                price=price,
            )
        else:
            fee_data = self._trading_fees["*"]
            if is_maker:
                fee_value = Decimal(fee_data["maker"])
            else:
                fee_value = Decimal(fee_data["taker"])
            fee = AddedToCostTradeFee(percent=fee_value)
        return fee

    async def _format_trading_rules(self, exchange_info_dict: Dict[str, Any]) -> List[TradingRule]:
        """
            Example:
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
        """
        trading_pair_rules = exchange_info_dict
        retval = []

        for rule in trading_pair_rules:
            try:
                if rule == 0:
                    continue
                trading_pair = utils.market_to_trading_pair(exchange_info_dict[rule]["market"]).replace("-PERP", "")
                rule_set = trading_pair_rules[rule]["book_info"]
                min_order_size = utils.convert_from_x18(rule_set.get("min_size"))
                min_price_increment = utils.convert_from_x18(rule_set.get("price_increment_x18"))
                min_base_amount_increment = utils.convert_from_x18(rule_set.get("size_increment"))

                retval.append(
                    TradingRule(trading_pair,
                                min_order_size=Decimal(min_order_size),
                                min_price_increment=Decimal(min_price_increment),
                                min_base_amount_increment=Decimal(min_base_amount_increment),
                                min_notional_size=Decimal("0.01"),
                                buy_order_collateral_token="USDC",
                                sell_order_collateral_token="USDC",
                                ))
                # TODO: This has long and short margin risks....
                self._margin_fractions.update({trading_pair: {"long": {}, "short": {}}})
                self._margin_fractions[trading_pair]["long"] = {
                    "initial": Decimal(utils.convert_from_x18(trading_pair_rules[rule].get("risk")["long_weight_initial_x18"])),
                    "maintenance": Decimal(utils.convert_from_x18(trading_pair_rules[rule].get("risk")["long_weight_maintenance_x18"])),
                }
                self._margin_fractions[trading_pair]["short"] = {
                    "initial": Decimal(utils.convert_from_x18(trading_pair_rules[rule].get("risk")["short_weight_initial_x18"])),
                    "maintenance": Decimal(utils.convert_from_x18(trading_pair_rules[rule].get("risk")["short_weight_maintenance_x18"])),
                }

            except Exception:
                self.logger().exception(f"Error parsing the trading pair rule {trading_pair_rules[rule].get('name')}. Skipping.")
        return retval

    async def _update_balances(self):
        # TODO: Review this to update for perps this needs to be really well updated.
        if not self._exchange_info:
            await self.build_exchange_info()

        local_asset_names = set(self._account_balances.keys())
        remote_asset_names = set()
        account_details, spot_product_map, perp_product_map = await self._get_account()
        available_balances = await self._get_account_max_withdrawable()
        # self.logger().warning(account)
        # self.logger().warning(available_balances)
        self._allocated_collateral_sum = s_decimal_0

        # Loop for all the balances returned for account
        for spot_balance in account_details["spot_balances"]:
            try:
                product_id = spot_balance["product_id"]
                # If we don't have it in our exchange defined list, we don't care
                if product_id not in self._exchange_info and product_id not in [0, 31]:
                    continue

                asset_name = "USDC"
                if product_id == 31:
                    asset_name = "USDT"
                total_balance = Decimal(utils.convert_from_x18(spot_balance["balance"]["amount"]))
                if product_id not in [0, 31]:
                    asset_name = self._exchange_info[product_id]["symbol"]

                available_balance = s_decimal_0
                if product_id in available_balances:
                    available_balance = available_balances[product_id]

                self._account_available_balances[asset_name] = available_balance
                self._account_balances[asset_name] = total_balance
                remote_asset_names.add(asset_name)
            except Exception as e:
                self.logger().warning(f"Spot Balance Error: {spot_balance} {e}")
                pass

        for perp_balance in account_details["perp_balances"]:
            try:
                pass
            except Exception as e:
                self.logger().warning(f"Perp Balance Error: {spot_balance} {e}")
                pass

        asset_names_to_remove = local_asset_names.difference(remote_asset_names)
        for asset_name in asset_names_to_remove:
            del self._account_available_balances[asset_name]
            del self._account_balances[asset_name]

    async def _update_positions(self):
        # NOTE: We can simulate transactions before update to ensure position
        account_details, spot_product_map, perp_product_map = await self._get_account()

        position_summaries = await self._get_position_summaries()

        for position in account_details["perp_balances"]:
            product_id = position["product_id"]
            if product_id not in self._exchange_info and product_id != 0:
                continue
            # self.logger().warning(self._exchange_market_info[self._domain])
            trading_pair = self._exchange_info[product_id]["symbol"]
            amount = Decimal(utils.convert_from_x18(position["balance"]["amount"]))
            position_side = PositionSide.LONG if amount > s_decimal_0 else PositionSide.SHORT
            pos_key = self._perpetual_trading.position_key(trading_pair, position_side)

            if amount != s_decimal_0:
                post_balance_amount = position_summaries[product_id]["amount"]
                mark_price = Decimal(utils.convert_from_x18(perp_product_map[product_id]["oracle_price_x18"]))
                unrealized_pnl = (
                    post_balance_amount * mark_price - position_summaries[product_id]["net_entry_unrealized"]
                ).quantize(Decimal("1.00"))
                entry_price = position_summaries[product_id]["entry_price"].quantize(Decimal("1.00"))
                initial_margin = Decimal(
                    utils.convert_from_x18(perp_product_map[product_id]["risk"]["long_weight_initial_x18"])
                )
                initial_leverage = Decimal("0.0")
                if initial_margin != Decimal("1.0"):
                    initial_leverage = (Decimal("1.0") / (Decimal("1.0") - initial_margin)).quantize(Decimal("10.0"))

                position = Position(
                    trading_pair=trading_pair,
                    position_side=position_side,
                    unrealized_pnl=unrealized_pnl,
                    entry_price=entry_price,
                    amount=amount,
                    leverage=initial_leverage,
                )
                self._perpetual_trading.set_position(pos_key, position)
            else:
                self._perpetual_trading.remove_position(pos_key)

            # TODO: Check mark price internal state vs latest from API request if not same then update.
            # TODO: Handle update existing_position.update_position(unrealized_pnl=Decimal(str(position_msg["unrealisedPnl"])))

    async def _place_order(
        self,
        order_id: str,
        trading_pair: str,
        amount: Decimal,
        trade_type: TradeType,
        order_type: OrderType,
        price: Decimal,
        position_action: PositionAction = PositionAction.NIL,
        **kwargs,
    ) -> Tuple[str, float]:
        # A positive amount means that this is a buy order, and a negative amount means this is a sell order.
        if trade_type == TradeType.SELL:
            amount = -amount
        trading_rules = self.trading_rules[trading_pair]
        amount_str = utils.convert_to_x18(amount, trading_rules.min_base_amount_increment)
        price_str = utils.convert_to_x18(price, trading_rules.min_price_increment)
        # reduce_only = position_action == PositionAction.CLOSE
        # TODO: Review to ensure this works?
        if order_type and order_type == OrderType.LIMIT_MAKER:
            _order_type = CONSTANTS.TIME_IN_FORCE_POSTONLY
        else:
            _order_type = CONSTANTS.TIME_IN_FORCE_GTC
        expiration = utils.generate_expiration(time.time(), order_type=_order_type)
        product_id = utils.trading_pair_to_product_id(trading_pair=trading_pair, exchange_market_info=self._exchange_info, is_perp=True)
        nonce = utils.generate_nonce(time.time())
        contract = self._exchange_info[product_id]["contract"]
        chain_id = CONSTANTS.CHAIN_IDS[self.domain]

        try:
            sender = utils.hex_to_bytes32(self.sender_address)
            raw_order = dict(
                sender=sender, priceX18=int(price_str), amount=int(amount_str), expiration=int(expiration), nonce=nonce
            )

            order = vertex_perpetual_eip712_structs.Order(
                sender=sender, priceX18=int(price_str), amount=int(amount_str), expiration=int(expiration), nonce=nonce
            )

            signature, digest = self.authenticator.sign_payload(order, contract, chain_id)
        except Exception as e:
            self.logger().exception(f"FAILED TO BUILD EIP712 STRUCT: {e}")
            raise e

        place_order = {
            "place_order": {
                "product_id": product_id,
                "order": {
                    "sender": self.sender_address,
                    "priceX18": price_str,
                    "amount": amount_str,
                    "expiration": expiration,
                    "nonce": str(nonce),
                },
                "signature": signature,
            }
        }

        try:
            order_result = await self._api_post(path_url=CONSTANTS.POST_PATH_URL, data=place_order, limit_id=CONSTANTS.PLACE_ORDER_METHOD)
            if order_result.get("status") == "failure":
                raise Exception(f"FAILED TO CREATE ORDER: {raw_order}, RESPONSE: {order_result}")

        except Exception as e:
            self.logger().exception(f"FAILED TO SUBMIT ORDER: {raw_order}, ERROR: {e}")
            raise e

        o_id = digest
        transact_time = int(time.time())
        return (o_id, transact_time)

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder):
        sender = utils.hex_to_bytes32(self.sender_address)
        product_id = utils.trading_pair_to_product_id(trading_pair=tracked_order.trading_pair, exchange_market_info=self._exchange_info, is_perp=True)
        nonce = utils.generate_nonce(time.time())
        endpoint_contract = CONSTANTS.CONTRACTS[self.domain]
        chain_id = CONSTANTS.CHAIN_IDS[self.domain]
        if tracked_order.exchange_order_id:
            order_id = tracked_order.exchange_order_id
        else:
            order_id = tracked_order.client_order_id

        order_id_bytes = utils.hex_to_bytes32(order_id)

        try:
            cancel = vertex_perpetual_eip712_structs.Cancellation(
                sender=sender, productIds=[int(product_id)], digests=[order_id_bytes], nonce=nonce
            )
            signature, digest = self.authenticator.sign_payload(cancel, endpoint_contract, chain_id)
        except Exception as e:
            self.logger().exception(f"FAILED TO BUILD EIP712 CANCEL STRUCT: {e}")

        cancel_orders = {
            "cancel_orders": {
                "tx": {
                    "sender": self.sender_address,
                    "productIds": [product_id],
                    "digests": [order_id],
                    "nonce": str(nonce),
                },
                "signature": signature,
            }
        }

        cancel_result = await self._api_post(path_url=CONSTANTS.POST_PATH_URL, data=cancel_orders, limit_id=CONSTANTS.CANCEL_ORDERS_METHOD)
        if cancel_result.get("status") == "failure":
            if cancel_result.get("error_code") and cancel_result["error_code"] == 2020:
                self._order_tracker._trigger_cancelled_event(tracked_order)
                self.logger().warning(f"Marked order canceled as the exchange holds no record: {order_id}")
                return True

        if isinstance(cancel_result, dict) and cancel_result["status"] == "success":
            return True
        return False

    async def _update_trading_fees(self):
        """
        Update fees information from the exchange
        """
        """
        {
        "status": "success",
        "data": {
            "taker_fee_rates_x18": [
            "0",
            "300000000000000",
            "200000000000000",
            "300000000000000",
            "200000000000000"
            ],
            "maker_fee_rates_x18": [
            "0",
            "0",
            "0",
            "0",
            "0"
            ],
            "liquidation_sequencer_fee": "250000000000000000",
            "health_check_sequencer_fee": "100000000000000000",
            "taker_sequencer_fee": "25000000000000000",
            "withdraw_sequencer_fees": [
            "10000000000000000",
            "40000000000000",
            "0",
            "600000000000000",
            "0"
            ]
        }
        }
        """
        response: Dict[str, Dict[str, Any]] = await self._api_get(
            path_url=CONSTANTS.QUERY_PATH_URL,
            params={
                "type": CONSTANTS.FEE_RATES_REQUEST_TYPE,
                "sender": self.sender_address,
            },
            is_auth_required=False,
            limit_id=CONSTANTS.FEE_RATES_REQUEST_TYPE,
        )

        if response is None or "failure" in response["status"]:
            self.logger().error("Received no response from Vertex for trading fees")

        taker_fees = {idx: fee_rate for idx, fee_rate in enumerate(response["data"]["taker_fee_rates_x18"])}
        maker_fees = {idx: fee_rate for idx, fee_rate in enumerate(response["data"]["maker_fee_rates_x18"])}

        for trading_pair in self._trading_pairs:
            product_id = utils.trading_pair_to_product_id(trading_pair=trading_pair, exchange_market_info=self._exchange_info, is_perp=True)
            if product_id == -1:
                continue
            self._trading_fees[trading_pair] = {
                "maker": Decimal(utils.convert_from_x18(maker_fees[product_id])),
                "taker": Decimal(utils.convert_from_x18(taker_fees[product_id])),
            }

    async def _user_stream_event_listener(self):
        """
        This functions runs in background continuously processing the events received from the exchange by the user
        stream data source. It keeps reading events from the queue until the task is interrupted.
        The events received are balance updates, order updates and trade events.
        """

        async for event_message in self._iter_user_event_queue():
            try:
                event_type = event_message.get("type")

                if event_type == CONSTANTS.FILL_EVENT_TYPE:
                    exchange_order_id = event_message.get("order_digest")
                    execution_type = (
                        OrderState.PARTIALLY_FILLED
                        if Decimal(utils.convert_from_x18(event_message["remaining_qty"])) > s_decimal_0
                        else OrderState.FILLED
                    )
                    original_amount = Decimal(utils.convert_from_x18(event_message["original_qty"]))
                    tracked_order = self._order_tracker.fetch_order(exchange_order_id=exchange_order_id)

                    # TODO: Review this for actually closing out the order / trade. I think if it's already executed... What happens now?
                    if tracked_order is not None:
                        position_action = (PositionAction.OPEN
                                           if (tracked_order.trade_type is TradeType.BUY and original_amount > Decimal("0.0")
                                               or tracked_order.trade_type is TradeType.SELL and original_amount < Decimal("0.0"))
                                           else PositionAction.CLOSE)
                        if execution_type in [OrderState.PARTIALLY_FILLED, OrderState.FILLED]:
                            # TODO: Review the calculations here as we need to calculate all of this in order to have it be correct.
                            # NOTE: We need to use abs amount of the amount for hummingbot
                            amount = abs(Decimal(utils.convert_from_x18(event_message["filled_qty"])))
                            price = Decimal(utils.convert_from_x18(event_message["price"]))
                            # TODO: We can integrate true account fees https://vertex-protocol.gitbook.io/docs/developer-resources/api/websocket-rest-api/queries/fee-rates
                            # TODO: If maker... 0
                            fee_amount = amount * Decimal("0.03")
                            fee = TradeFeeBase.new_perpetual_fee(
                                fee_schema=self.trade_fee_schema(),
                                position_action=position_action,
                                flat_fees=[TokenAmount(amount=fee_amount, token="USDC")],
                            )
                            trade_update = TradeUpdate(
                                trade_id=str(event_message["timestamp"]),
                                client_order_id=tracked_order.client_order_id,
                                exchange_order_id=str(exchange_order_id),
                                trading_pair=tracked_order.trading_pair,
                                fee=fee,
                                fill_base_amount=amount,
                                fill_quote_amount=amount * price,
                                fill_price=price,
                                fill_timestamp=int(event_message["timestamp"]) * 1e-9,
                            )
                            # self.logger().warn(f"STREAM TRADE UPDATE: {trade_update}")
                            self._order_tracker.process_trade_update(trade_update)
                        # self.logger().warn(f"SHOULD UPDATE ORDER TO: {execution_type}")
                        order_update = OrderUpdate(
                            trading_pair=tracked_order.trading_pair,
                            update_timestamp=int(event_message["timestamp"]) * 1e-9,
                            new_state=execution_type,
                            client_order_id=tracked_order.client_order_id,
                            exchange_order_id=str(exchange_order_id),
                        )
                        # self.logger().warn(f"STREAM ORDER UPDATE: {order_update}")
                        self._order_tracker.process_order_update(order_update=order_update)

                elif event_type == CONSTANTS.POSITION_CHANGE_EVENT_TYPE:
                    # NOTE: Just call update balance....
                    await self._update_balances()
                else:
                    self.logger().error(f"Unexpected event `{event_type}` in user stream listener loop.")

            except asyncio.CancelledError:
                self.logger().error(f"An Asyncio.CancelledError occurs when process message: {event_message}.", exc_info=True)
                raise
            except Exception:
                self.logger().error("Unexpected error in user stream listener loop.", exc_info=True)
                await self._sleep(5.0)

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        trade_updates = []
        if order.exchange_order_id is not None:
            exchange_order_id = order.exchange_order_id
            trading_pair = order.trading_pair
            product_id = utils.trading_pair_to_product_id(trading_pair=order.trading_pair, exchange_market_info=self._exchange_info, is_perp=True)
            if product_id == -1:
                raise IOError(f"Unable to fetch for unknown product {trading_pair} {exchange_order_id}")
            matches_response = await self._api_post(
                path_url=CONSTANTS.INDEXER_PATH_URL,
                data={
                    "matches": {
                        "product_ids": [product_id],
                        "subaccount": self.sender_address,
                        # "max_time": None,
                        # "limit": 500
                    }
                },
                limit_id=CONSTANTS.INDEXER_PATH_URL,
            )

            matches_data = matches_response.get("matches", [])
            if matches_data is not None:
                for trade in matches_data:
                    # NOTE: Vertex returns all orders and matches.
                    # Not the order / trades we're looking for.
                    if trade["digest"] != order.exchange_order_id:
                        continue
                    # self.logger().warn(f"FOUND TRADE: {trade}")
                    exchange_order_id = str(trade["digest"])
                    original_amount = Decimal(utils.convert_from_x18(trade["order"]["amount"]))
                    # position_action = PositionAction.OPEN
                    # TODO: Review this...
                    position_action = (PositionAction.OPEN if original_amount > Decimal("0.0") else PositionAction.CLOSE)
                    # Matches can be composed of multiple trade transactions..
                    # TODO: Rebuild this with each transaction being a trade event with the whole order being serviced...
                    # We'll need to calcuate the fee for each trade (we know the net total from the start)
                    # https://vertex-protocol.gitbook.io/docs/developer-resources/api/indexer-api/matches
                    submission_idx = str(trade["submission_idx"])
                    trade_fee = utils.convert_from_x18(trade["fee"])
                    # trade_amount = utils.convert_from_x18(trade["order"]["amount"])
                    fee = TradeFeeBase.new_perpetual_fee(
                        fee_schema=self.trade_fee_schema(),
                        position_action=position_action,
                        percent_token="USDC",
                        # TODO: Review the additional fees for sequencer and taker etc... https://vertex-protocol.gitbook.io/docs/basics/fees#sequencer-fees
                        flat_fees=[TokenAmount(amount=Decimal(trade_fee), token="USDC")],
                    )

                    fill_base_amount = utils.convert_from_x18(trade["base_filled"])
                    converted_price = utils.convert_from_x18(trade["order"]["priceX18"])
                    fill_quote_amount = utils.convert_from_x18(trade["base_filled"])
                    # NOTE: Matches can be composed of multiple trade transactions..
                    matches_transactions_data = matches_response.get("txs", [])
                    trade_timestamp = int(time.time())
                    for transaction in matches_transactions_data:
                        # TODO: Clean this up, we shouldn't have to loop in a loop, instead build dict or array with everything
                        if str(transaction["submission_idx"]) != submission_idx:
                            continue
                        # All this to get a timestamp!!!
                        trade_timestamp = transaction["timestamp"]
                        trade_update = TradeUpdate(
                            trade_id=submission_idx,
                            client_order_id=order.client_order_id,
                            exchange_order_id=exchange_order_id,
                            trading_pair=trading_pair,
                            fee=fee,
                            fill_base_amount=abs(Decimal(fill_base_amount)),
                            fill_quote_amount=Decimal(converted_price) * abs(Decimal(fill_quote_amount)),
                            fill_price=Decimal(converted_price),
                            fill_timestamp=int(trade_timestamp),
                        )
                        trade_updates.append(trade_update)

        return trade_updates

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        live_order = True
        updated_order_data = {"status": "failure", "data": {"unfilled_amount": 100000000000, "amount": 1000000000000}}
        try:
            order_request_response = await self._api_get(
                path_url=CONSTANTS.QUERY_PATH_URL,
                params={
                    "type": CONSTANTS.ORDER_REQUEST_TYPE,
                    "product_id": utils.trading_pair_to_product_id(trading_pair=tracked_order.trading_pair, exchange_market_info=self._exchange_info, is_perp=True),
                    "digest": tracked_order.exchange_order_id,
                },
                limit_id=CONSTANTS.ORDER_REQUEST_TYPE,
            )
            if order_request_response.get("status") == "failure":
                self.logger().warning(f"Error fetching order status for {tracked_order.client_order_id} {tracked_order.exchange_order_id}")
            else:
                updated_order_data = order_request_response
        except Exception as e:
            self.logger().warning(f"ERROR ORDER VERTEX: {e}")
            raise e

        if updated_order_data.get("status") == "failure":
            live_order = False
            try:
                # Get order from the indexer
                indexed_order_data = await self._api_post(
                    path_url=CONSTANTS.INDEXER_PATH_URL, data={"orders": {"digests": [tracked_order.exchange_order_id]}}, limit_id=CONSTANTS.INDEXER_PATH_URL
                )
                orders = indexed_order_data.get("orders", [])
                if len(orders) > 0:
                    updated_order_data["data"] = orders[0]
                    updated_order_data["data"]["unfilled_amount"] = float(updated_order_data["data"]["amount"]) - float(
                        updated_order_data["data"]["base_filled"]
                    )

            except Exception as e:
                self.logger().warning(f"ERROR ORDER INDEXER: {e}")
        unfilled_amount = Decimal(utils.convert_from_x18(updated_order_data["data"]["unfilled_amount"]))
        order_amount = Decimal(utils.convert_from_x18(updated_order_data["data"]["amount"]))
        filled_amount = abs(Decimal(order_amount - unfilled_amount))

        if filled_amount == Decimal("0.0"):
            new_state = OrderState.OPEN
        if filled_amount > Decimal("0.0"):
            new_state = OrderState.PARTIALLY_FILLED
        # Default to canceled if this is queried against indexer
        if not live_order:
            new_state = OrderState.CANCELED
        if unfilled_amount == Decimal("0.0"):
            if live_order:
                new_state = OrderState.FILLED
            else:
                # Override default canceled with complete if complete
                new_state = OrderState.COMPLETED
        # self.logger().warn(f"NEW ORDER STATE: {new_state}")
        order_update = OrderUpdate(
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=str(tracked_order.exchange_order_id),
            trading_pair=tracked_order.trading_pair,
            update_timestamp=int(time.time()),
            new_state=new_state,
        )
        # self.logger().warn(f"ORDER UPDATE: {order_update}")
        return order_update

    async def _initialize_trading_pair_symbol_map(self):
        try:
            exchange_info = await self.build_exchange_info()
            self._initialize_trading_pair_symbols_from_exchange_info(exchange_info=exchange_info)
        except Exception:
            self.logger().exception("There was an error requesting exchange info.")

    async def _update_trading_rules(self):
        exchange_info = await self.build_exchange_info()
        trading_rules_list = await self._format_trading_rules(exchange_info)
        self._trading_rules.clear()
        for trading_rule in trading_rules_list:
            self._trading_rules[trading_rule.trading_pair] = trading_rule
        self._initialize_trading_pair_symbols_from_exchange_info(exchange_info=exchange_info)

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Dict[str, Any]):
        mapping = bidict()
        for product_id in filter(utils.is_exchange_information_valid, exchange_info):
            trading_pair = exchange_info[product_id]["market"]
            # NOTE: USDC is an asset, however it doesn't have a "market"
            if product_id == 0:
                continue
            base = trading_pair.split("/")[0].replace("-", "")
            quote = trading_pair.split("/")[1]
            mapping[trading_pair] = combine_to_hb_trading_pair(base=base, quote=quote)
        self._set_trading_pair_symbol_map(mapping)

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        product_id = utils.trading_pair_to_product_id(trading_pair=trading_pair, exchange_market_info=self._exchange_info, is_perp=True)
        # self.logger().warn("GETTING LAST TRADED PRICE")
        try:
            matches_response = await self._api_post(
                path_url=CONSTANTS.INDEXER_PATH_URL,
                data={"matches": {"product_ids": [product_id], "limit": 5}},
                limit_id=CONSTANTS.INDEXER_PATH_URL,
            )
            matches = matches_response.get("matches", [])
            if matches:
                last_price = float(utils.convert_from_x18(matches[0]["order"]["priceX18"]))
                # self.logger().warn(f"GOT LAST TRADED PRICE: {last_price}")
                return last_price
                # return float(utils.convert_from_x18(matches[0]["order"]["priceX18"]))
        except Exception as e:
            self.logger().warning(f"Failed to get last traded price, using bid instead, error: {e}")

        params = {"type": CONSTANTS.MARKET_PRICE_REQUEST_TYPE, "product_id": product_id}
        resp_json = await self._api_request(
            method=RESTMethod.GET,
            path_url=CONSTANTS.QUERY_PATH_URL,
            params=params,
            limit_id=CONSTANTS.MARKET_PRICE_REQUEST_TYPE,
        )
        trading_rules = self.trading_rules[trading_pair]
        mid_price = float(
            str(
                (
                    (
                        Decimal(utils.convert_from_x18(resp_json["data"]["bid_x18"]))
                        + Decimal(utils.convert_from_x18(resp_json["data"]["ask_x18"]))
                    )
                    / Decimal("2.0")
                ).quantize(trading_rules.min_price_increment)
            )
        )
        return mid_price

    async def _api_request(
        self,
        path_url,
        method: RESTMethod = RESTMethod.GET,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
        is_auth_required: bool = False,
        return_err: bool = False,
        limit_id: Optional[str] = None,
        **kwargs,
    ) -> Dict[str, Any]:
        last_exception = None
        rest_assistant = await self._web_assistants_factory.get_rest_assistant()
        url = web_utils.public_rest_url(path_url, domain=self.domain)
        local_headers = None
        if method == RESTMethod.POST:
            local_headers = {"Content-Type": "application/json"}
        for _ in range(2):
            try:
                request_result = await rest_assistant.execute_request(
                    url=url,
                    params=params,
                    data=data,
                    method=method,
                    is_auth_required=is_auth_required,
                    return_err=return_err,
                    headers=local_headers,
                    throttler_limit_id=limit_id if limit_id else CONSTANTS.ALL_ENDPOINTS_LIMIT,
                )
                return request_result
            except IOError as request_exception:
                last_exception = request_exception
                raise

        # Failed even after the last retry
        raise last_exception

    async def _get_account(self):
        # NOTE: https://vertex-protocol.gitbook.io/docs/developer-resources/api/websocket-rest-api/queries/subaccount-info
        sender_address = self.sender_address
        response: Dict[str, Dict[str, Any]] = await self._api_get(
            path_url=CONSTANTS.QUERY_PATH_URL,
            params={"type": CONSTANTS.SUBACCOUNT_INFO_REQUEST_TYPE, "subaccount": sender_address},
            limit_id=CONSTANTS.SUBACCOUNT_INFO_REQUEST_TYPE,
        )

        if "data" not in response:
            raise IOError(f"Unable to get subaccount info for subaccount {sender_address}")
        # TODO: Handle error codes?
        spot_product_map = {product['product_id']: product for product in response["data"]["spot_products"]}
        perp_product_map = {product['product_id']: product for product in response["data"]["perp_products"]}

        account_details = response["data"]

        return account_details, spot_product_map, perp_product_map

    async def build_exchange_info(self):
        exchange_info = await self._api_get(path_url=self.trading_pairs_request_path)
        symbol_map = await self._get_symbols()
        contract_info = await self._get_contracts()
        self._exchange_info = {}

        symbol_data = {}
        for product in symbol_map:
            symbol_data.update({product["product_id"]: product["symbol"]})

        product_data = {}
        for product in exchange_info["data"]["spot_products"]:
            if product["product_id"] in symbol_data:
                try:
                    product_id = int(product["product_id"])
                    # NOTE: Hardcoded USDC
                    product.update({"symbol": f"{symbol_data[product_id]}"})
                    product.update({"market": f"{symbol_data[product_id]}/USDC"})
                    product.update({"contract": f"{contract_info[product_id]}"})
                    product_data.update({product_id: product})
                except Exception:
                    pass
        for product in exchange_info["data"]["perp_products"]:
            if product["product_id"] in symbol_data:
                try:
                    product_id = int(product["product_id"])
                    # NOTE: Hardcoded USDC
                    product.update({"symbol": f"{symbol_data[product_id]}"})
                    product.update({"market": f"{symbol_data[product_id]}/USDC"})
                    product.update({"contract": f"{contract_info[product_id]}"})
                    product_data.update({product_id: product})
                except Exception:
                    pass

        self._exchange_info = product_data
        return product_data

    async def _get_symbols(self):
        response = await self._api_get(path_url=CONSTANTS.SYMBOLS_PATH_URL)

        if response is None or "status" in response:
            raise IOError("Unable to get Vertex symbols")

        self._symbols = response

        return response

    async def _get_account_max_withdrawable(self):
        sender_address = self.sender_address
        available_balances = {}
        trading_pairs = self._trading_pairs

        params = {
            "type": CONSTANTS.MAX_WITHDRAWABLE_REQUEST_TYPE,
            "product_id": 0,
            "sender": sender_address,
            "spot_leverage": str(self._use_spot_leverage).lower(),
        }
        response = await self._api_get(path_url=CONSTANTS.QUERY_PATH_URL, params=params)

        if response is None or "failure" in response["status"] or "data" not in response:
            raise IOError(f"Unable to get available balance of product {0} for {sender_address}")

        available_balances.update({0: Decimal(utils.convert_from_x18(response["data"]["max_withdrawable"]))})

        if len(self._trading_pairs) == 0:
            trading_pairs = []
            for product_id in self._exchange_info:
                if product_id != 0:
                    trading_pairs.append(self._exchange_info[product_id]["market"])
        for trading_pair in trading_pairs:
            # TODO: We want a conversion from perp to spot product_id...
            product_id = utils.trading_pair_to_product_id(
                trading_pair=trading_pair, exchange_market_info=self._exchange_info, is_perp=True
            )

            spot_product_id = utils.perp_to_spot_product_id(trading_pair=trading_pair, exchange_market_info=self._exchange_info)
            params = {
                "type": CONSTANTS.MAX_WITHDRAWABLE_REQUEST_TYPE,
                "product_id": spot_product_id,
                "sender": sender_address,
                "spot_leverage": str(self._use_spot_leverage).lower(),
            }

            # TODO: In here we need to map for perps...
            response = await self._api_get(path_url=CONSTANTS.QUERY_PATH_URL, params=params)

            if response is None or "failure" in response["status"] or "data" not in response:
                raise IOError(f"Unable to get available balance of product {product_id} for {sender_address}")

            available_balances.update(
                {product_id: Decimal(utils.convert_from_x18(response["data"]["max_withdrawable"]))}
            )

        return available_balances

    async def _get_contracts(self):
        response = await self._api_get(
            path_url=CONSTANTS.QUERY_PATH_URL, params={"type": CONSTANTS.CONTRACTS_REQUEST_TYPE}
        )

        if response is None or "failure" in response["status"] or "data" not in response:
            raise IOError("Unable to get Vertex contracts")

        # NOTE: List indexed to be matached according to product_id
        contracts = response["data"]["book_addrs"]

        self._contracts = contracts

        return contracts

    async def _get_fee_rates(self):
        sender_address = self.sender_address
        response: Dict[str, Dict[str, Any]] = await self._api_get(
            path_url=CONSTANTS.QUERY_PATH_URL,
            params={
                "type": CONSTANTS.FEE_RATES_REQUEST_TYPE,
                "sender": sender_address,
            },
            is_auth_required=False,
            limit_id=CONSTANTS.FEE_RATES_REQUEST_TYPE,
        )

        if response is None or "failure" in response["status"] or "data" not in response:
            raise IOError(f"Unable to get trading fees sender address {sender_address}")

        return response["data"]

    async def _get_position_summaries(self):
        sender_address = self.sender_address
        data = {
            "summary": {
                "subaccount": sender_address,
                # "timestamp": None
            }
        }
        # TODO: Catch...?
        response: Dict[str, Dict[str, Any]] = await self._api_post(path_url=CONSTANTS.INDEXER_PATH_URL, data=data)
        if "events" not in response:
            raise IOError(f"Unable to get position summaries for subaccount {sender_address}")

        position_summaries = {}
        # source: https://vertex-protocol.gitbook.io/docs/developer-resources/api/indexer-api/summary
        # TODO: Review # net_funding_cumulative
        for event in response["events"]:
            position_summary = {}
            # NOTE: We don't care about anything but perps...
            if "perp" not in event["post_balance"]:
                continue
            amount = Decimal(utils.convert_from_x18(event["post_balance"]["perp"]["balance"]["amount"]))
            if amount != s_decimal_0:
                value = Decimal(utils.convert_from_x18(event["net_entry_unrealized"]))
                net_funding_cumulative = Decimal(utils.convert_from_x18(event["net_funding_cumulative"]))
                net_funding_unrealized = Decimal(utils.convert_from_x18(event["net_funding_unrealized"]))
                entry_price = abs(value / amount).quantize(Decimal("1.00"))
                position_summary[event["product_id"]] = {
                    "net_funding_cumulative": net_funding_cumulative,
                    "net_funding_unrealized": net_funding_unrealized,
                    "entry_price": entry_price,
                    "amount": amount,
                    "net_entry_unrealized": value,
                }
                position_summaries.update(position_summary)

        return position_summaries

    async def _get_funding_rate(self, product_id: int):
        data = {"funding_rate": {"product_id": product_id}}

        response: Dict[str, Any] = await self._api_post(
            path_url=CONSTANTS.INDEXER_PATH_URL, data=data, limit_id=CONSTANTS.INDEXER_PATH_URL
        )

        if "product_id" not in response:
            raise IOError(f"Unable to get funding rate for product id {product_id}")

        return response

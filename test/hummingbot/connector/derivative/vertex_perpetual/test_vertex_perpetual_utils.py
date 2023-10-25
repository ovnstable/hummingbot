import random
from decimal import Decimal
from unittest import TestCase

from hummingbot.connector.derivative.vertex_perpetual import (
    vertex_perpetual_constants as CONSTANTS,
    vertex_perpetual_utils as utils,
)


class VertexPerpeturalUtilTestCases(TestCase):
    def test_hex_to_bytes32(self):
        hex_string = "0x5cc7c91690b2cbaee19a513473d73403e13fb431"  # noqa: mock
        expected_bytes = b"\\\xc7\xc9\x16\x90\xb2\xcb\xae\xe1\x9aQ4s\xd74\x03\xe1?\xb41\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"  # noqa: mock
        self.assertEqual(expected_bytes, utils.hex_to_bytes32(hex_string))

    def test_convert_timestamp(self):
        timestamp = 1685989014506281744
        expected_ts = 1685989014506281744 / 1e9
        self.assertEqual(expected_ts, utils.convert_timestamp(timestamp))

    def test_trading_pair_to_product_id(self):
        trading_pair = "wBTC-USDC"
        expected_id = 1
        self.assertEqual(expected_id, utils.trading_pair_to_product_id(trading_pair))
        missing_trading_pair = "ABC-XYZ"
        expected_missing_id = -1
        self.assertEqual(expected_missing_id, utils.trading_pair_to_product_id(missing_trading_pair))

    def test_market_to_trading_pair(self):
        market = "wBTC/USDC"
        expected_trading_pair = "wBTC-USDC"
        self.assertEqual(expected_trading_pair, utils.market_to_trading_pair(market))

    def test_product_id_to_trading_pair(self):
        product_id = 1
        expected_trading_pair = "wBTC-USDC"
        self.assertEqual(expected_trading_pair, utils.product_id_to_trading_pair(product_id))

    def test_convert_from_x18(self):
        data_numeric = 26369000000000000000000
        expected_numeric = "26369"
        self.assertEqual(expected_numeric, utils.convert_from_x18(data_numeric))

        data_dict = {
            "bids": [["26369000000000000000000", "294000000000000000"]],
            "asks": [["26370000000000000000000", "551000000000000000"]],
        }
        expected_dict = {
            "bids": [["26369", "0.294"]],
            "asks": [["26370", "0.551"]],
        }
        self.assertEqual(expected_dict, utils.convert_from_x18(data_dict))

    def test_convert_to_x18(self):
        data_numeric = 26369.123
        expected_numeric = "26369000000000000000000"
        self.assertEqual(expected_numeric, utils.convert_to_x18(data_numeric, Decimal("1")))

        data_dict = {
            "bids": [[26369.0, 0.294]],
            "asks": [[26370.0, 0.551]],
        }
        expected_dict = {
            "bids": [["26369000000000000000000", "294000000000000000"]],
            "asks": [["26370000000000000000000", "551000000000000000"]],
        }
        self.assertEqual(expected_dict, utils.convert_to_x18(data_dict))

    def test_generate_expiration(self):
        timestamp = 1685989011.1215873
        expected_gtc = "1685990011"
        expected_ioc = "4611686020113377915"
        expected_fok = "9223372038540765819"
        expected_postonly = "13835058056968153723"
        self.assertEqual(expected_gtc, utils.generate_expiration(timestamp, CONSTANTS.TIME_IN_FORCE_GTC))
        self.assertEqual(expected_ioc, utils.generate_expiration(timestamp, CONSTANTS.TIME_IN_FORCE_IOC))
        self.assertEqual(expected_fok, utils.generate_expiration(timestamp, CONSTANTS.TIME_IN_FORCE_FOK))
        self.assertEqual(
            expected_postonly, utils.generate_expiration(timestamp, CONSTANTS.TIME_IN_FORCE_POSTONLY)
        )

    def test_generate_nonce(self):
        timestamp = 1685989011.1215873
        expiry_ms = 90
        expected_nonce = 1767887707697054351
        random.seed(42)
        self.assertEqual(expected_nonce, utils.generate_nonce(timestamp, expiry_ms))

    def test_convert_address_to_sender(self):
        address = "0xbbee07b3e8121227afcfe1e2b82772246226128e"  # noqa: mock
        expected_sender = "0xbbee07b3e8121227afcfe1e2b82772246226128e64656661756c740000000000"  # noqa: mock
        self.assertEqual(expected_sender, utils.convert_address_to_sender(address))

    def test_is_exchange_information_valid(self):
        self.assertTrue(utils.is_exchange_information_valid({}))

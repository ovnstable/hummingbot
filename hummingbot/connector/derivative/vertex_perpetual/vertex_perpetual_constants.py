from typing import Any, Dict

# A single source of truth for constant variables related to the exchange
from hummingbot.core.api_throttler.data_types import LinkedLimitWeightPair, RateLimit
from hummingbot.core.data_type.in_flight_order import OrderState

# Max order id allowed by Vertex is 32. We need to configure it in 22 to unify the timestamp and client_id
# components in the order id, because Vertex uses the last 9 characters or the order id to generate the
# exchange order id (the last 9 characters would be part of the client id if it is not mixed with the timestamp).
# But Vertex only uses milliseconds for the timestamp in the exchange order id, causing duplicated exchange order ids
# when running strategies with multi level orders created very fast.
MAX_ORDER_ID_LEN = 22

HEARTBEAT_TIME_INTERVAL = 30.0

ORDER_BOOK_REFRESH_RATE = 30

ORDER_BOOK_DEPTH = 100

VERSION = "0.0.1"

EXCHANGE_NAME = "vertex_perpetual"

DEFAULT_DOMAIN = "vertex_perpetual"
TESTNET_DOMAIN = "vertex_perpetual_testnet"

BASE_URLS = {
    DEFAULT_DOMAIN: "https://prod.vertexprotocol-backend.com",
    TESTNET_DOMAIN: "https://test.vertexprotocol-backend.com",
}

WSS_URLS = {
    DEFAULT_DOMAIN: "wss://prod.vertexprotocol-backend.com",
    TESTNET_DOMAIN: "wss://test.vertexprotocol-backend.com",
}

CONTRACTS = {
    DEFAULT_DOMAIN: "0xbbee07b3e8121227afcfe1e2b82772246226128e",
    TESTNET_DOMAIN: "0x5956d6f55011678b2cab217cd21626f7668ba6c5",
}

INDEXER_URLS = {
    DEFAULT_DOMAIN: "https://archive.prod.vertexprotocol.com/v1",
    TESTNET_DOMAIN: "https://archive.test.vertexprotocol.com/v1"
}

CHAIN_IDS = {
    DEFAULT_DOMAIN: 42161,
    TESTNET_DOMAIN: 421613,
}

HBOT_BROKER_ID = ""

SIDE_BUY = "BUY"
SIDE_SELL = "SELL"

TIME_IN_FORCE_GTC = "GTC"  # Good till cancelled
TIME_IN_FORCE_IOC = "IOC"  # Immediate or cancel
TIME_IN_FORCE_FOK = "FOK"  # Fill or kill
TIME_IN_FORCE_POSTONLY = "POSTONLY"  # PostOnly

# API PATHS
POST_PATH_URL = "/execute"
QUERY_PATH_URL = "/query"
INDEXER_PATH_URL = "/indexer"
SYMBOLS_PATH_URL = "/symbols"
WS_PATH_URL = "/ws"
WS_SUBSCRIBE_PATH_URL = "/subscribe"

# POST METHODS
PLACE_ORDER_METHOD = "place_order"
PLACE_ORDER_METHOD_NO_LEVERAGE = "place_order_no_leverage"
CANCEL_ORDERS_METHOD = "cancel_orders"
CANCEL_ALL_METHOD = "cancel_product_orders"

# REST QUERY API TYPES
STATUS_REQUEST_TYPE = "status"
ORDER_REQUEST_TYPE = "order"
SUBACCOUNT_INFO_REQUEST_TYPE = "subaccount_info"
MARKET_LIQUIDITY_REQUEST_TYPE = "market_liquidity"
ALL_PRODUCTS_REQUEST_TYPE = "all_products"
MARKET_PRICE_REQUEST_TYPE = "market_price"
FEE_RATES_REQUEST_TYPE = "fee_rates"
CONTRACTS_REQUEST_TYPE = "contracts"
SUBACCOUNT_ORDERS_REQUEST_TYPE = "subaccount_orders"
MAX_WITHDRAWABLE_REQUEST_TYPE = "max_withdrawable"

# WS API ENDPOINTS
WS_SUBSCRIBE_METHOD = "subscribe"
TOB_TOPIC_EVENT_TYPE = "best_bid_offer"
POSITION_CHANGE_EVENT_TYPE = "position_change"
SNAPSHOT_EVENT_TYPE = "market_liquidity"
TRADE_EVENT_TYPE = "trade"
DIFF_EVENT_TYPE = "book_depth"
FILL_EVENT_TYPE = "fill"
POSITION_CHANGE_EVENT_TYPE = "position_change"

# OrderStates
ORDER_STATE = {
    "PendingNew": OrderState.PENDING_CREATE,
    "New": OrderState.OPEN,
    "Filled": OrderState.FILLED,
    "PartiallyFilled": OrderState.PARTIALLY_FILLED,
    "Canceled": OrderState.CANCELED,
    "Rejected": OrderState.FAILED,
}

# Vertex has multiple pools for API request limits
# TODO: Currently Vertex has API call rates on the methods, not the url endpoints. So we'll have to revisit this to ensure this is
# constructed to pass via the throttler in hummingbot
# Any call increases call rate in ALL pool, so e.g. a query/execute call will contribute to both ALL and query/execute pools.
ALL_ENDPOINTS_LIMIT = "All"
RATE_LIMITS = [
    RateLimit(limit_id=ALL_ENDPOINTS_LIMIT, limit=600, time_interval=10),
    # TODO: REMOVE DUMMY FOR JUST GETTING THINGS TO WORK
    RateLimit(
        limit_id=INDEXER_PATH_URL, limit=60, time_interval=1, linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT)]
    ),
    RateLimit(
        limit_id=STATUS_REQUEST_TYPE, limit=60, time_interval=1, linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT)]
    ),
    RateLimit(
        limit_id=ORDER_REQUEST_TYPE,
        limit=60,
        time_interval=1,
        # NOTE: No weight for weight of 1...
        linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT)],
    ),
    RateLimit(
        limit_id=SUBACCOUNT_INFO_REQUEST_TYPE,
        limit=60,
        time_interval=10,
        weight=10,
        linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT)],
    ),
    RateLimit(
        limit_id=MARKET_LIQUIDITY_REQUEST_TYPE,
        limit=60,
        time_interval=1,
        # NOTE: No weight for weight of 1...
        linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT)],
    ),
    RateLimit(
        limit_id=ALL_PRODUCTS_REQUEST_TYPE,
        limit=12,
        time_interval=1,
        weight=5,
        linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT)],
    ),
    RateLimit(
        limit_id=MARKET_PRICE_REQUEST_TYPE,
        limit=60,
        time_interval=1,
        # NOTE: No weight for weight of 1...
        linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT)],
    ),
    RateLimit(
        limit_id=FEE_RATES_REQUEST_TYPE,
        limit=30,
        time_interval=1,
        weight=2,
        linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT)],
    ),
    # TODO: This is a bit complex as it's looking for leverage or type of order...
    # Review https://vertex-protocol.gitbook.io/docs/developer-resources/api/websocket-rest-api/executes/place-order
    # CAN BREAK THIS INTO SEVERAL DIFFERENT RATE LIMITS AND DO IT AT ORDER PLACEMENT...
    RateLimit(
        limit_id=PLACE_ORDER_METHOD,
        limit=10,
        time_interval=1,
        linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT)],
    ),
    # TODO: This is also a bit complex as the rate limit depends on total digests, which we're sending 1 at a time...
    # https://vertex-protocol.gitbook.io/docs/developer-resources/api/websocket-rest-api/executes/cancel-orders
    RateLimit(
        limit_id=CANCEL_ORDERS_METHOD,
        limit=10,
        time_interval=1,
        linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT)],
    ),
    # TODO: This also has a restriction based on the product ids, we typically only send one, but...
    # https://vertex-protocol.gitbook.io/docs/developer-resources/api/websocket-rest-api/executes/cancel-product-orders
    RateLimit(
        limit_id=CANCEL_ALL_METHOD,
        limit=2,
        time_interval=1,
        linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT)],
    ),
]

"""
https://vertex-protocol.gitbook.io/docs/developer-resources/api/api-errors
"""
# TODO: I wanted to have these here for our ability to parse and understand the errors..
ERRORS: Dict[Any, Any] = {
    100: {
        "msg": "invalid price: not divisible by increment",
        "method": "Place Order",
        "type": "execute",
    },
    101: {
        "msg": "invalid amount: not divisible by increment",
        "method": "Place Order",
        "type": "execute",
    },
    102: {
        "msg": "invalid amount: zero",
        "method": "Place Order",
        "type": "execute",
    },
    103: {
        "msg": "invalid expiration: already expired",
        "method": "Place Order",
        "type": "execute",
    },
    104: {
        "msg": "invalid amount: too small",
        "method": "Place Order",
        "type": "execute",
    },
    105: {
        "msg": "max orders limit reached",
        "method": "Place Order",
        "type": "execute",
    },
    106: {
        "msg": "unhealthy order",
        "method": "Place Order",
        "type": "execute",
    },
    107: {
        "msg": "Order price must be within a range of 20% to 500% of oracle price",
        "method": "Place Order",
        "type": "execute",
    },
    108: {
        "msg": "spot_leverage is false, but placing this order can cause borrows",
        "method": "Place Order",
        "type": "execute",
    },
    2008: {
        "msg": "order is post only, but it crosses book",
        "method": "Place Order",
        "type": "execute",
    },
    110: {
        "msg": "order type not supported!",
        "method": "Place Order",
        "type": "execute",
    },
    111: {
        "msg": "fill or kill order cannot be fully filled",
        "method": "Place Order",
        "type": "execute",
    },
    112: {
        "msg": "invalid taker order",
        "method": "Place Order",
        "type": "execute",
    },
    113: {
        "msg": "execute received after recv_time",
        "method": "Place Order, Cancel Orders",
        "type": "execute",
    },
    114: {
        "msg": "execute received more than 100 seconds before recv_time",
        "method": "Place Order, Cancel Orders",
        "type": "execute",
    },
    115: {
        "msg": "digest already exists",
        "method": "Place Order, Cancel Product Orders",
        "type": "execute",
    },
    116: {
        "msg": "trying to cancel order for another subaccount",
        "method": "Cancel Orders",
        "type": "execute",
    },
    2020: {
        "msg": "order with digest {order_digest} not found",
        "method": "Cancel Orders",
        "type": "execute",
    },
    118: {
        "msg": "digests and productIds must be the same length",
        "method": "Cancel Orders",
        "type": "execute",
    },
    119: {
        "msg": "market not found for given product id",
        "method": "Place Order, Cancel Orders, Cancel Product Orders, Mint LP, Market Price, Order, Subaccount Orders, Market Liquidity, Max Order Size, Max LP Mintable",
        "description": "The product id provided does not have a market. e.g: product_id=0",
        "type": "execute, query",
    },
    120: {
        "msg": "spot_leverage is false, but executing this withdraw can cause borrows",
        "method": "Withdraw Collateral",
        "type": "execute",
    },
    # TODO: Continue from here...
    121: {
        "msg": "Invalid product id",
        "method": "Place Order",
        "type": "execute",
    },
    122: {
        "msg": "product_id must be spot",
        "method": "Place Order",
        "type": "execute",
    },
    123: {
        "msg": "spot_leverage is false, but executing this mint can cause borrows",
        "method": "Place Order",
        "type": "execute",
    },
    124: {
        "msg": "Order not found",
        "method": "Place Order",
        "type": "execute",
    },
    125: {
        "msg": "Invalid nonce",
        "method": "Place Order",
        "type": "execute",
    },
    126: {
        "msg": "Linking signer to the same address again is not allowed",
        "method": "Place Order",
        "type": "execute",
    },
    127: {
        "msg": "Must have account value more than 5 USDC to enable single signature sessions.",
        "method": "Place Order",
        "type": "execute",
    },
    128: {
        "msg": "Address risk is too high",
        "method": "Place Order",
        "type": "execute",
    },
    129: {
        "msg": "Address screening risk check pending for this address",
        "method": "Place Order",
        "type": "execute",
    },
    130: {
        "msg": "Address has never deposited into Vertex",
        "method": "Place Order",
        "type": "execute",
    },
    131: {
        "msg": "cannot specify digests with subaccount or product_ids",
        "method": "Orders",
        "type": "indexer",
    },
    132: {
        "msg": "cannot specify more digests than limit",
        "method": "Orders",
        "type": "indexer",
    },
    133: {
        "msg": "must specify subaccount",
        "method": "Orders",
        "type": "indexer",
    },
    134: {
        "msg": "IP is from blocked location: {locale}",
        "method": "Blocked",
        "type": "general",
    },
    135: {
        "msg": "No locale found for ip: {IP}",
        "method": "Blocked",
        "type": "general",
    },
    136: {
        "msg": "Too Many Requests!",
        "method": "Requests",
        "type": "general",
    },
}

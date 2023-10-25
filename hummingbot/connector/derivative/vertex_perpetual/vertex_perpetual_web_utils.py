import time
from typing import Optional

import hummingbot.connector.derivative.vertex_perpetual.vertex_perpetual_constants as CONSTANTS
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


def public_rest_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    """
    Creates a full URL for provided public REST endpoint
    :param path_url: a public REST endpoint
    :param domain: the Vertex domain to connect to ("mainnet" or "testnet"). The default value is "mainnet"
    :return: the full URL to the endpoint
    """
    return CONSTANTS.BASE_URLS[domain] + path_url


def private_rest_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    """
    Creates a full URL for provided REST endpoint

    :param path_url: a private REST endpoint
    :param domain: the domain to connect to ("main" or "testnet"). The default value is "main"

    :return: the full URL to the endpoint
    """
    return public_rest_url(path_url=path_url, domain=domain)


# TODO: Review
def build_api_factory(
    throttler: Optional[AsyncThrottler] = None,
    auth: Optional[AuthBase] = None,
) -> WebAssistantsFactory:
    throttler = throttler or create_throttler()
    api_factory = WebAssistantsFactory(throttler=throttler, auth=auth)
    return api_factory


def build_api_factory_without_time_synchronizer_pre_processor(throttler: AsyncThrottler) -> WebAssistantsFactory:
    api_factory = WebAssistantsFactory(throttler=throttler)
    return api_factory


def create_throttler() -> AsyncThrottler:
    return AsyncThrottler(CONSTANTS.RATE_LIMITS)


# NOTE: We don't use throttler or domain
async def get_current_server_time(
    throttler: AsyncThrottler, domain: str = CONSTANTS.DEFAULT_DOMAIN
) -> float:
    server_time = float(time.time())
    return server_time

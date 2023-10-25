import time
from typing import Any, Tuple

import sha3
from coincurve import PrivateKey
from eip712_structs import make_domain
from eth_utils import big_endian_to_int

import hummingbot.connector.derivative.vertex_perpetual.vertex_perpetual_constants as CONSTANTS
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTRequest, WSRequest


def keccak_hash(x):
    return sha3.keccak_256(x).digest()


class VertexPerpetualAuth(AuthBase):

    def __init__(self, vertex_perpetual_arbitrum_address: str, vertex_perpetual_arbitrum_private_key: str):
        self.sender_address = vertex_perpetual_arbitrum_address
        self.private_key = vertex_perpetual_arbitrum_private_key

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        """
        This method is intended to configure a rest request to be authenticated. Vertex does not use this
        functionality.
        :param request: the request to be configured for authenticated interaction
        """
        return request

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        """
        This method is intended to configure a websocket request to be authenticated. Vertex does not use this
        functionality.
        :param request: the request to be configured for authenticated interaction
        """
        return request  # pass-through

    def get_referral_code_headers(self):
        """
        Generates referral headers when supported by Vertex
        :return: a dictionary of auth headers
        """
        headers = {"referer": CONSTANTS.HBOT_BROKER_ID}
        return headers

    def sign_payload(self, payload: Any, contract: str, chain_id: int) -> Tuple[str, str]:
        domain = make_domain(name="Vertex", version=CONSTANTS.VERSION, chainId=chain_id, verifyingContract=contract)

        signable_bytes = payload.signable_bytes(domain)
        # Digest for order tracking in Hummingbot
        digest = self.generate_digest(signable_bytes)

        pk = PrivateKey.from_hex(self.private_key)
        signature = pk.sign_recoverable(signable_bytes, hasher=keccak_hash)

        v = signature[64] + 27
        r = big_endian_to_int(signature[0:32])
        s = big_endian_to_int(signature[32:64])

        final_sig = r.to_bytes(32, "big") + s.to_bytes(32, "big") + v.to_bytes(1, "big")
        return f"0x{final_sig.hex()}", digest

    def generate_digest(self, signable_bytes: bytearray) -> str:
        return f"0x{keccak_hash(signable_bytes).hex()}"

    def _time(self):
        return time.time()

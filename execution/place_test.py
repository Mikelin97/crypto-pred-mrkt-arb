from py_clob_client.client import ClobClient
from py_clob_client.clob_types import (
    OrderArgs,
    OrderType,
    PartialCreateOrderOptions,
    RequestArgs,
)
from py_clob_client.headers.headers import create_level_2_headers
from py_clob_client.order_builder.constants import BUY
from py_clob_client.utilities import order_to_json
from py_clob_client.http_helpers.helpers import post
from py_clob_client.endpoints import POST_ORDER
import os
import time
import dotenv

dotenv.load_dotenv()

host: str = "https://clob.polymarket.com"
key: str = os.getenv("PRIVATE_KEY") #This is your Private Key. Export from https://reveal.magic.link/polymarket or from your Web3 Extension
chain_id: int = 137 #No need to adjust this
POLYMARKET_PROXY_ADDRESS: str = os.getenv("POLYMARKET_PROXY_ADDRESS") #This is the address listed below your profile picture when using the Polymarket site.

#Select from the following 3 initialization options to match your login method, and remove any unused lines so only one client is initialized.


### Initialization of a client using a Polymarket Proxy associated with an Email/Magic account. If you login with your email use this example.
# client = ClobClient(host, key=key, chain_id=chain_id, signature_type=1, funder=POLYMARKET_PROXY_ADDRESS)

### Initialization of a client using a Polymarket Proxy associated with a Browser Wallet(Metamask, Coinbase Wallet, etc)
client = ClobClient(host, key=key, chain_id=chain_id, signature_type=2, funder=POLYMARKET_PROXY_ADDRESS)

### Initialization of a client that trades directly from an EOA. (If you don't know what this means, you're not using it)
# client = ClobClient(host, key=key, chain_id=chain_id)

## Create and sign a limit order buying 5 tokens for 0.010c each
#Refer to the API documentation to locate a tokenID: https://docs.polymarket.com/developers/gamma-markets-api/fetch-markets-guide


def timed_post_order(client: ClobClient, order, order_type: OrderType = OrderType.GTC):
    """Mirror ClobClient.post_order while recording per-step latency."""
    timings: list[tuple[str, float]] = []

    start = time.perf_counter()
    client.assert_level_2_auth()
    timings.append(("assert_level_2_auth", (time.perf_counter() - start) * 1000))

    start = time.perf_counter()
    body = order_to_json(order, client.creds.api_key, order_type)
    timings.append(("order_to_json", (time.perf_counter() - start) * 1000))

    start = time.perf_counter()
    request_args = RequestArgs(method="POST", request_path=POST_ORDER, body=body)
    timings.append(("build_request_args", (time.perf_counter() - start) * 1000))

    start = time.perf_counter()
    headers = create_level_2_headers(client.signer, client.creds, request_args)
    timings.append(("create_level_2_headers", (time.perf_counter() - start) * 1000))

    builder_headers = None
    if client.can_builder_auth():
        start = time.perf_counter()
        builder_headers = client._generate_builder_headers(request_args, headers)
        timings.append(("builder_headers", (time.perf_counter() - start) * 1000))

    start = time.perf_counter()
    resp = post(
        f"{client.host}{POST_ORDER}",
        headers=builder_headers or headers,
        data=body,
    )
    timings.append(("http_post", (time.perf_counter() - start) * 1000))

    print("post_order breakdown (ms):")
    total = 0.0
    for label, latency in timings:
        print(f"  {label}: {latency:.2f}")
        total += latency
    print(f"  total_tracked: {total:.2f}")

    return resp

start = time.perf_counter()
creds = client.create_or_derive_api_creds()
print(f"create_api_creds latency: {(time.perf_counter() - start) * 1000:.2f} ms")

start = time.perf_counter()
client.set_api_creds(creds)
print(f"set_api_creds latency: {(time.perf_counter() - start) * 1000:.2f} ms")

order_args = OrderArgs(
    price=0.1,
    size=5.0,
    side=BUY,
    token_id="74643785274431202653559375289615825875786488765132690442260156602594571459673", #Token ID you want to purchase goes here. Example token: 114304586861386186441621124384163963092522056897081085884483958561365015034812 ( Xi Jinping out in 2025, YES side )
)

start = time.perf_counter()
signed_order = client.create_order(order_args, )
print(f"create_order latency: {(time.perf_counter() - start) * 1000:.2f} ms")

## GTC(Good-Till-Cancelled) Order
resp = timed_post_order(client, signed_order, OrderType.GTC)
print(resp)

import dotenv
import os 
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, OrderType
from py_clob_client.order_builder.constants import BUY


dotenv.load_dotenv()

PRIVATE_KEY = os.getenv("PRIVATE_KEY")
POLYMARKET_PROXY_ADDRESS = os.getenv("POLYMARKET_PROXY_ADDRESS")




host: str = "https://clob.polymarket.com"
key: str = PRIVATE_KEY
chain_id: int = 137 #No need to adjust this
POLYMARKET_PROXY_ADDRESS: str = POLYMARKET_PROXY_ADDRESS



### Initialization of a client using a Polymarket Proxy associated with a Browser Wallet(Metamask, Coinbase Wallet, etc)
client = ClobClient(host, key=key, chain_id=chain_id, signature_type=2, funder=POLYMARKET_PROXY_ADDRESS)


## Create and sign a limit order buying 5 tokens for 0.010c each
#Refer to the API documentation to locate a tokenID: https://docs.polymarket.com/developers/gamma-markets-api/fetch-markets-guide

client.set_api_creds(client.create_or_derive_api_creds()) 

print(client.get_ok())


# order_args = OrderArgs(
#     price=0.01,
#     size=5.0,
#     side=BUY,
#     token_id="", #Token ID you want to purchase goes here. Example token: 114304586861386186441621124384163963092522056897081085884483958561365015034812 ( Xi Jinping out in 2025, YES side )
# )


# signed_order = client.create_order(order_args)

# ## GTC(Good-Till-Cancelled) Order
# resp = client.post_order(signed_order, OrderType.GTC)
# print(resp)
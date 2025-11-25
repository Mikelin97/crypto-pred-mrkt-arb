import argparse
import sys
from pathlib import Path

FILE_DIR = Path(__file__).resolve().parent
REPO_ROOT = FILE_DIR.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.append(str(REPO_ROOT))

import dotenv
from py_clob_client.clob_types import OrderType

from shared.execution import ExecutionClient

dotenv.load_dotenv()


def main() -> None:
    parser = argparse.ArgumentParser(description="Simple Polymarket order submission tester.")
    parser.add_argument("--token-id", required=True, help="Token ID to trade.")
    parser.add_argument("--side", choices=("buy", "sell"), default="buy")
    parser.add_argument("--price", type=float, required=True)
    parser.add_argument("--size", type=float, required=True)
    parser.add_argument(
        "--order-type",
        choices=("GTC", "FOK", "GTD", "FAK"),
        default="GTC",
        help="Order type as defined by Polymarket.",
    )
    args = parser.parse_args()

    client = ExecutionClient.from_env()
    order_args = client.build_order_args(
        price=args.price,
        size=args.size,
        side=args.side,
        token_id=args.token_id,
    )
    order_type = getattr(OrderType, args.order_type)
    result = client.place_order(order_args, order_type=order_type, timed=True)

    print("Order response:", result.response)
    if result.timings_ms:
        print("Latency breakdown (ms):")
        for label, latency in result.timings_ms:
            print(f"  {label}: {latency:.2f}")


if __name__ == "__main__":
    main()

import argparse
import json
import sys
import time
from urllib.request import urlopen


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--miner-ip", required=True)
    parser.add_argument("--miner-port", required=True)
    parser.add_argument("--expected-version", required=True)
    args = parser.parse_args()

    for i in range(10):
        try:
            time.sleep(i)
            response = urlopen(
                f"http://{args.miner_ip}:{args.miner_port}/version", timeout=2
            )
            data = json.loads(response.read())
            returned_version = data["miner_version"]
            if returned_version == args.expected_version:
                return 0
            print(
                f"Miner {args.miner_ip}:{args.miner_port} version mismatch: "
                f"returned={returned_version} expected={args.expected_version}"
            )
        except Exception as e:
            print(repr(e))

    return 1


if __name__ == "__main__":
    sys.exit(main())

import argparse
import json
import sys
import time
from contextlib import suppress
from urllib.request import urlopen


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--miner-ip", required=True)
    parser.add_argument("--miner-port", required=True)
    parser.add_argument("--expected-version", required=True)
    args = parser.parse_args()

    for i in range(10):
        with suppress(Exception):
            time.sleep(i)
            response = urlopen(
                f"http://{args.miner_ip}:{args.miner_port}/version", timeout=2
            )
            data = json.loads(response.read())
            if data["miner_version"] == args.expected_version:
                return 0

    return 1


if __name__ == "__main__":
    sys.exit(main())

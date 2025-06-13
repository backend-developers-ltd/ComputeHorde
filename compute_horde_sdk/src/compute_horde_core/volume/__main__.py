import argparse
import asyncio
import json
import pathlib
from typing import Any

from pydantic import TypeAdapter

from ._downloader import VolumeDownloader
from ._models import Volume


def parse_args() -> argparse.Namespace:
    def json_file(path: str) -> Any:
        with open(path) as f:
            return json.load(f)

    parser = argparse.ArgumentParser(
        prog="python -m compute_horde_core.volume", description="Run downloader for a given volume specification."
    )
    parser.add_argument("input", nargs="+", type=json_file, help="JSON input file")
    parser.add_argument("-d", "--dir", default=pathlib.Path("/volume"), type=pathlib.Path, help="Volume directory.")

    return parser.parse_args()


async def main() -> None:
    args = parse_args()
    for volume in args.input:
        model: Volume = TypeAdapter(Volume).validate_python(volume)
        downloader = VolumeDownloader.for_volume(model)
        await downloader.download(args.dir)


asyncio.run(main())

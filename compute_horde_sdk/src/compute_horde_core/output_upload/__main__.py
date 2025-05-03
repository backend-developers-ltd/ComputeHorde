import argparse
import asyncio
import json
import pathlib
from typing import Any

from pydantic import TypeAdapter

from ._models import OutputUpload
from ._uploader import OutputUploader


def parse_args() -> argparse.Namespace:
    def json_file(path: str) -> Any:
        with open(path) as f:
            return json.load(f)

    parser = argparse.ArgumentParser(
        prog="python -m compute_horde_core.output_upload",
        description="Run uploader for a given output-upload specification.",
    )
    parser.add_argument("input", nargs="+", type=json_file, help="JSON input file")
    parser.add_argument("-d", "--dir", default=pathlib.Path("/output"), type=pathlib.Path, help="Output directory.")

    return parser.parse_args()


async def main() -> None:
    args = parse_args()
    for volume in args.input:
        model: OutputUpload = TypeAdapter(OutputUpload).validate_python(volume)
        downloader = OutputUploader.for_upload_output(model)
        await downloader.upload(args.dir)


asyncio.run(main())

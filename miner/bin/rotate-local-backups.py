#!/usr/bin/env python3

# Copyright (c) 2020, Reef Technologies, BSD 3-Clause License

import argparse
from pathlib import Path


def parse_arguments():
    parser = argparse.ArgumentParser(description="Keep all but N most recent files in a directory")
    parser.add_argument("file_count", help="How many last files to keep", type=int)
    return parser.parse_args()


def rotate_backups(path, file_count):
    files = [(f.stat().st_mtime, f) for f in Path(path).iterdir() if f.is_file()]
    files.sort()
    files = files[:-file_count]
    if files:
        print(f"Removing {len(files)} old files")
        for mtime, f in files:
            f.unlink()
    else:
        print("No old files to remove")


if __name__ == "__main__":
    parser_result = parse_arguments()
    rotate_backups(".backups", parser_result.file_count)

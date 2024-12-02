import logging
import os
import re
import time
from collections import defaultdict
from collections.abc import Sequence
from datetime import datetime
from glob import glob
from pathlib import Path

from django.conf import settings

from compute_horde.receipts.schemas import (
    Receipt,
)
from compute_horde.receipts.store.base import BaseReceiptStore

# 5 minutes per page
PAGE_TIME_MOD = 60 * 5

logger = logging.getLogger(__name__)


class LocalFilesystemPagedReceiptStore(BaseReceiptStore):
    def __init__(self):
        super().__init__()
        self.pages_directory: Path = Path(settings.LOCAL_RECEIPTS_ROOT)

    @staticmethod
    def current_page_at(dt: datetime) -> int:
        return int(dt.timestamp() // PAGE_TIME_MOD)

    @staticmethod
    def active_page_id() -> int:
        return int(time.time()) // PAGE_TIME_MOD

    @staticmethod
    def receipt_page(receipt: Receipt) -> int:
        return LocalFilesystemPagedReceiptStore.current_page_at(receipt.payload.timestamp)

    def store(self, receipts: Sequence[Receipt]) -> None:
        """
        Append receipts to the store.
        """
        pages: defaultdict[int, list[Receipt]] = defaultdict(list)
        for receipt in receipts:
            page = self.receipt_page(receipt)
            pages[page].append(receipt)
        for page, receipts_in_page in pages.items():
            self._append_to_page(receipts_in_page, page)

    def page_filepath(self, page: int) -> Path:
        """
        Find the filepath under which given pagefile should be found.
        Does not check whether it actually exists or not.
        """
        return self.pages_directory / f"{page}.jsonl"

    def _append_to_page(self, receipts: Sequence[Receipt], page: int) -> None:
        """
        Write new receipts to the end of specified page.
        Does not check whether the receipts actually belong on this page.
        """
        with open(self.page_filepath(page), "a") as pagefile:
            # TODO: copy -> append -> move
            for r in receipts:
                pagefile.write(r.model_dump_json())
                pagefile.write("\n")

    def delete_page(self, page: int) -> None:
        page_filepath = self.page_filepath(page)
        try:
            os.unlink(page_filepath)
        except FileNotFoundError:
            pass

    def get_available_pages(self) -> list[int]:
        """
        Return IDs of all existing pages.
        """
        pagefiles = self._get_available_page_filepaths()
        pages: list[int] = []
        pattern = re.compile(r"(\d+)\.jsonl$")
        for pagefile in pagefiles:
            match = pattern.search(str(pagefile))
            if match:
                pages.append(int(match[1]))
        return pages

    def _get_available_page_filepaths(self) -> list[Path]:
        """
        Return filepaths of all existing pages.
        """
        pagefiles = glob(str(self.pages_directory / "*.jsonl"))
        return [Path(pagefile) for pagefile in pagefiles]

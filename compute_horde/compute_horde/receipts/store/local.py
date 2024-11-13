import re
from glob import glob
from pathlib import Path
from typing import Sequence, DefaultDict

from compute_horde.receipts.schemas import (
    Receipt,
)
from django.conf import settings
from mypy.memprofile import defaultdict

from compute_horde.receipts.store.base import BaseReceiptStore

PageID = int


class LocalFilesystemPagedReceiptStore(BaseReceiptStore):
    def __init__(self):
        super().__init__()
        self.pages_directory: Path = Path(settings.LOCAL_RECEIPTS_ROOT)
        self.modulus = 60 * 60

    def store(self, receipts: Sequence[Receipt]) -> None:
        """
        Append receipts to the store.
        """
        pages: DefaultDict[PageID, list[Receipt]] = defaultdict(list)
        for receipt in receipts:
            page = self._receipt_page_id(receipt)
            pages[page].append(receipt)
        for page, receipts_in_page in pages.items():
            self._append_to_page(receipts_in_page, page)

    def page_filepath(self, page: PageID) -> Path:
        """
        Find the filepath under which given pagefile should be found.
        Does not whether it actually exists or not.
        """
        return self.pages_directory / f"{page}.jsonl"

    def _receipt_page_id(self, receipt: Receipt) -> PageID:
        """
        Map a receipt to its page ID.
        """
        return int(receipt.payload.timestamp.timestamp()) // self.modulus

    def _append_to_page(self, receipts: Sequence[Receipt], page: PageID) -> None:
        """
        Write new receipts to the end of specified page.
        Does not check whether the receipts actually belong on this page.
        TODO: Make this thread- and cross-process-safe
        """
        with open(self.page_filepath(page), "a") as pagefile:
            pagefile.writelines(r.model_dump_json() for r in receipts)

    def evoke_page(self, page: PageID):
        """
        Evoke the page from storage if it exists.
        """
        self.page_filepath(page).unlink(missing_ok=True)

    def get_pages(self) -> list[PageID]:
        """
        Return IDs of all existing pages.
        """
        pagefiles = glob(str(self.pages_directory / "*.jsonl"))
        pages: list[PageID] = []
        pattern = re.compile(r"(\d+)\.jsonl$")
        for pagefile in pagefiles:
            match = pattern.match(pagefile)
            if match:
                pages.append(int(match[0]))
        return pages

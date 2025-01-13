import gzip
import logging
import os
import re
import shutil
import tempfile
from collections import defaultdict
from collections.abc import Sequence
from datetime import datetime
from glob import glob
from pathlib import Path

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.utils import timezone

from compute_horde.receipts.schemas import (
    Receipt,
)
from compute_horde.receipts.store.base import BaseReceiptStore

"""
Considerations:
- smaller pages make for more (smaller) batches when downloading them for the first time (and we need ~5 hours of pages)
- pages get copied over on write, larger pages may impact efficiency of this
- we keep up with the 2 latest pages based on time and the clock may be out of sync between miners and validators
"""
PAGE_TIME = 60 * 5  # 5 minutes
N_ACTIVE_PAGES = 2

logger = logging.getLogger(__name__)


class LocalFilesystemPagedReceiptStore(BaseReceiptStore):
    def __init__(self):
        super().__init__()
        if not getattr(settings, "LOCAL_RECEIPTS_ROOT", ""):
            raise ImproperlyConfigured("Required settings.py setting missing: LOCAL_RECEIPTS_ROOT")
        self.pages_directory = Path(settings.LOCAL_RECEIPTS_ROOT)  # type: ignore
        self.pages_directory.mkdir(parents=True, exist_ok=True)

    @classmethod
    def current_page(cls) -> int:
        """
        Get current page ID
        """
        return cls.current_page_at(timezone.now())

    @staticmethod
    def current_page_at(dt: datetime) -> int:
        """
        Calculate what the current page was at given time
        """
        return int(dt.timestamp() // PAGE_TIME)

    def store(self, receipts: Sequence[Receipt]) -> None:
        """
        Append receipts to the store.
        """
        pages: defaultdict[int, list[Receipt]] = defaultdict(list)
        for receipt in receipts:
            pages[self.current_page()].append(receipt)
        for page, receipts_in_page in pages.items():
            self._append_to_page(receipts_in_page, page)

    def page_filepath(self, page: int) -> Path:
        """
        Find the filepath under which given pagefile should be found.
        Does not check whether it actually exists or not.
        """
        return self.pages_directory / f"{page}.jsonl"

    def archive_filepath(self, page: int) -> Path:
        """
        Find the filepath under which given pagefile's archive should be found.
        Does not check whether it actually exists or not.
        """
        return self.pages_directory / f"{page}.jsonl.gz"

    def delete_page(self, page: int) -> None:
        """
        Deletes the page file from the file system if it exists.
        Does nothing otherwise.
        """
        try:
            os.unlink(self.page_filepath(page))
        except FileNotFoundError:
            pass
        try:
            os.unlink(self.archive_filepath(page))
        except FileNotFoundError:
            pass

    def evict(self, cutoff: datetime) -> None:
        self.delete_pages_older_than(cutoff)

    def delete_pages_older_than(self, older_than: int | datetime) -> None:
        if isinstance(older_than, datetime):
            older_than = self.current_page_at(older_than)

        old_pages = [p for p in self.get_available_pages() if p < older_than]

        for old_page in old_pages:
            try:
                self.delete_page(old_page)
            except Exception:
                logger.exception("Error while deleting page %s", old_page, exc_info=True)

    def archive_old_pages(self) -> None:
        """
        Create archives for all old pages if they don't exist yet.
        Skips active pages as these can be still written to.
        """
        current_page = self.current_page()
        upper_cutoff = current_page - N_ACTIVE_PAGES
        pages_to_archive = [p for p in self.get_available_pages() if p <= upper_cutoff]
        for page in pages_to_archive:
            archive_filepath = self.archive_filepath(page)
            if archive_filepath.exists():
                continue
            self.do_archive_page(page)

    def do_archive_page(self, page: int) -> None:
        """
        Packs given page and creates an additional archive file used by nginx to serve the page.
        """
        with open(self.page_filepath(page), "rb") as page_file:
            with gzip.open(self.archive_filepath(page), "wb") as archive_file:
                shutil.copyfileobj(page_file, archive_file)

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

    def _append_to_page(self, receipts: Sequence[Receipt], page: int) -> None:
        """
        Write new receipts to the specified page.
        For read safety this will copy the page file first, append to the copy and do a swap with the original.
        """
        page_filepath = self.page_filepath(page)
        page_filepath.touch(exist_ok=True)
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_page_filepath = Path(tmpdir) / "tmp_page.jsonl"
            shutil.copyfile(page_filepath, tmp_page_filepath)
            with open(tmp_page_filepath, "a") as pagefile:
                for r in receipts:
                    pagefile.write(r.model_dump_json())
                    pagefile.write("\n")
            shutil.move(tmp_page_filepath, page_filepath)

    def _get_available_page_filepaths(self) -> list[Path]:
        """
        Return filepaths of all existing pages.
        """
        pagefiles = glob(str(self.pages_directory / "*.jsonl"))
        return [Path(pagefile) for pagefile in pagefiles]

import gzip
import logging
import shutil
from pathlib import Path

from django.core.management import BaseCommand

from compute_horde.receipts.store.local import LocalFilesystemPagedReceiptStore
from compute_horde.receipts.store.current import receipts_store

logger = logging.getLogger(__name__)

LIVE_PAGES_COUNT = 2


def compress_receipt_pages(except_last: int = LIVE_PAGES_COUNT):
    """
    Compresses receipt page files with gzip. Leaves the original files in place.
    If a receipt page file has a .gz version, nginx will automatically serve that instead of the raw file.
    This is fine for pages that will not change anymore, but we need to exclude a couple of latest "live" pages.
    """
    if not isinstance(receipts_store, LocalFilesystemPagedReceiptStore):
        raise Exception("This only works with LocalFilesystemPagedReceiptStore."
                        f" - got {receipts_store.__class__.__name__}")

    pages = sorted(receipts_store.get_available_pages())
    current_page = receipts_store.current_page()
    ignored_pages = pages[-except_last:] if except_last else []

    # Also exclude "future" pages. They should not be there in first place, so this script should not touch them.
    ignored_pages.extend((page for page in pages if page > current_page))

    for page in pages:
        page_filepath = receipts_store.page_filepath(page)
        compressed_filepath = Path(str(page_filepath) + ".gz")
        if compressed_filepath.exists():
            logger.info(f"Skipping page {page} - already exists")
            continue
        if page in ignored_pages:
            logger.info(f"Skipping page {page} - too recent")
            continue
        logger.info(f"Compressing page {page} -> {compressed_filepath}")
        with open(page_filepath, 'rb') as f_in:
            with gzip.open(compressed_filepath, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)


class Command(BaseCommand):
    def handle(self, *args, **options):
        compress_receipt_pages()

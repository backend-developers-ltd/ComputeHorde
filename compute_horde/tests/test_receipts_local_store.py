from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import pytest
from django.core.exceptions import ImproperlyConfigured
from django.test import override_settings
from freezegun import freeze_time

from compute_horde.receipts.store.local import (
    N_ACTIVE_PAGES,
    PAGE_TIME,
    LocalFilesystemPagedReceiptStore,
)
from tests.utils import random_receipt


@pytest.mark.parametrize("bad_value", [None, ""])
def test_exits_when_settings_missing(bad_value):
    with override_settings(LOCAL_RECEIPTS_ROOT=bad_value):
        with pytest.raises(ImproperlyConfigured):
            LocalFilesystemPagedReceiptStore().store([random_receipt()])


def test_creates_directory():
    with TemporaryDirectory() as tmpdir:
        subdir = Path(tmpdir) / "subdir"
        with override_settings(LOCAL_RECEIPTS_ROOT=str(subdir)):
            store = LocalFilesystemPagedReceiptStore()
            store.store([random_receipt()])

        assert subdir.exists()


def test_page_advances_with_time(time_freezer):
    initial_page = LocalFilesystemPagedReceiptStore.current_page()
    time_freezer.tick(delta=PAGE_TIME)
    next_page = LocalFilesystemPagedReceiptStore.current_page()
    assert next_page == initial_page + 1


def test_receipts_are_written_to_current_page(local_store, receipts_directory):
    with patch.object(LocalFilesystemPagedReceiptStore, "current_page") as current_page:
        for i in range(1, 6):
            current_page.return_value = i
            local_store.store([random_receipt() for _ in range(10)])

    for i in range(1, 6):
        assert (receipts_directory / f"{i}.jsonl").exists()


def test_receipts_are_readable(local_store):
    current_page = LocalFilesystemPagedReceiptStore.current_page()
    current_page_filepath = local_store.page_filepath(current_page)
    expected_receipts = []
    for _ in range(5):
        batch_to_write = [random_receipt() for _ in range(5)]
        local_store.store(batch_to_write)
        expected_receipts.extend(batch_to_write)
        with open(current_page_filepath, "rb") as pagefile:
            for file_line, receipt in zip(pagefile, expected_receipts):
                read_receipt = type(receipt).model_validate_json(file_line)
                assert read_receipt == receipt


def test_archives_correct_pages(local_store):
    with patch.object(LocalFilesystemPagedReceiptStore, "current_page") as current_page:
        for i in range(1, 10):
            current_page.return_value = i
            local_store.store([random_receipt()])

        local_store.archive_old_pages()

    for i in range(1, 10 - N_ACTIVE_PAGES):
        archive_filepath = local_store.archive_filepath(i)
        assert archive_filepath.exists()

    for i in range(10 - N_ACTIVE_PAGES + 1, 10):
        archive_filepath = local_store.archive_filepath(i)
        assert not archive_filepath.exists()


def test_lists_available_pages(local_store):
    with patch.object(LocalFilesystemPagedReceiptStore, "current_page") as current_page:
        expected_pages = [*range(1, 123, 7)]
        for page in expected_pages:
            current_page.return_value = page
            local_store.store([random_receipt()])

        assert sorted(local_store.get_available_pages()) == expected_pages


def test_deletes_old_pages(local_store):
    with patch.object(LocalFilesystemPagedReceiptStore, "current_page") as current_page:
        for page_to_create in range(1, 10):  # 1-9 inclusive
            current_page.return_value = page_to_create
            local_store.store([random_receipt()])

        assert sorted(local_store.get_available_pages()) == [*range(1, 10)]

        local_store.delete_pages_older_than(5)
        assert sorted(local_store.get_available_pages()) == [*range(5, 10)]


@pytest.fixture(autouse=True)
def time_freezer():
    with freeze_time("2024-12-01T00:00:00Z") as freezer:
        yield freezer


@pytest.fixture()
def receipts_directory():
    with TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture()
def local_store(receipts_directory: Path):
    with override_settings(LOCAL_RECEIPTS_ROOT=str(receipts_directory)):
        yield LocalFilesystemPagedReceiptStore()

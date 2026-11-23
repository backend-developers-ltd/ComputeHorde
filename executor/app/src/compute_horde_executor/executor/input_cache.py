import hashlib
import os
import re
import shutil
import sqlite3
import stat
from collections.abc import Iterator
from contextlib import closing, contextmanager
from pathlib import Path
from time import time

import httpx
from compute_horde_core.untrusted_files import open_directory, split_relative_path
from compute_horde_core.volume import (
    HuggingfaceVolume,
    InlineVolume,
    SingleFileVolume,
    VolumeDownloadFailed,
    ZipUrlVolume,
)


class InputCache:
    """A host-local cache. The executor manager serializes jobs on each host."""

    def __init__(self, budget: int, root: Path = Path("/tmp/executor-cache")):
        self.root = root
        self.budget = budget
        self.mounts: dict[str, Path] = {}

    @contextmanager
    def _index(self) -> Iterator[sqlite3.Connection]:
        self.root.mkdir(exist_ok=True)
        with closing(sqlite3.connect(self.root / "index.sqlite", isolation_level=None)) as db:
            db.row_factory = sqlite3.Row
            db.executescript("""
                CREATE TABLE IF NOT EXISTS entries (
                    digest TEXT PRIMARY KEY, completed REAL, expires REAL, size INTEGER, ctime_ns INTEGER
                );
                CREATE INDEX IF NOT EXISTS expiry ON entries(expires);
                CREATE TABLE IF NOT EXISTS sweep (id INTEGER PRIMARY KEY, last_run REAL);
            """)
            yield db

    @staticmethod
    def _file_stat(path: Path) -> os.stat_result | None:
        try:
            st = path.lstat()
        except FileNotFoundError:
            return None
        if not stat.S_ISREG(st.st_mode) or st.st_nlink != 1:
            raise VolumeDownloadFailed("Unexpected file in input cache")
        return st

    def _remove(self, db: sqlite3.Connection, digest: str) -> None:
        if not re.fullmatch("[0-9a-f]{64}", digest):
            raise VolumeDownloadFailed("Invalid input cache index")
        path = self.root / digest
        if self._file_stat(path):
            path.unlink()
        db.execute("DELETE FROM entries WHERE digest = ?", (digest,))

    def sweep(self) -> None:
        # A host that has never cached anything has no maintenance work.
        if not self.root.exists():
            return
        with self._index() as db:
            used = db.execute("SELECT coalesce(sum(size), 0) FROM entries").fetchone()[0]
            if used > self.budget:
                for digest, size in db.execute(
                    "SELECT digest, size FROM entries WHERE size > 0 ORDER BY expires"
                ).fetchall():
                    self._remove(db, digest)
                    used -= size
                    if used <= self.budget:
                        break

            now = time()
            previous = db.execute("SELECT last_run FROM sweep WHERE id = 1").fetchone()
            if previous and 0 <= now - previous[0] < 300:
                return
            partial = self.root / ".partial"
            if self._file_stat(partial):
                partial.unlink()
            for (digest,) in db.execute(
                "SELECT digest FROM entries WHERE expires <= ? ORDER BY expires LIMIT 128", (now,)
            ).fetchall():
                self._remove(db, digest)
            db.execute("INSERT OR REPLACE INTO sweep VALUES (1, ?)", (now,))

    def create_mount_targets(self, directory: Path) -> None:
        # Create targets last so ordinary children and archive contents cannot mask a bind.
        for name in self.mounts:
            target = directory / name
            if target.resolve() != target:
                raise VolumeDownloadFailed("Symlink in cached input destination")
            target.parent.mkdir(parents=True, exist_ok=True)
            target.touch(exist_ok=False)

    @contextmanager
    def prepare(
        self,
        volume: HuggingfaceVolume | InlineVolume | SingleFileVolume | ZipUrlVolume,
        directory: Path,
    ) -> Iterator[bool]:
        """Yield whether the runner needs to download this input."""
        if volume.relative_path:
            path = Path(volume.relative_path)
            target = directory / path
            if (
                path.is_absolute()
                or ".." in path.parts
                or "\0" in volume.relative_path
                or target.resolve() != target
            ):
                raise VolumeDownloadFailed("Invalid input path")
        if not isinstance(volume, SingleFileVolume) or not volume.cache_ttl_seconds:
            yield True
            return

        try:
            split_relative_path(volume.relative_path)
        except ValueError as e:
            raise VolumeDownloadFailed("Invalid cached input path", str(e)) from e
        assert volume.cache_key is not None
        digest = hashlib.sha256(volume.cache_key.encode()).hexdigest()
        path = self.root / digest
        if volume.relative_path in self.mounts:
            raise VolumeDownloadFailed("Conflicting cached input destinations")

        with self._index() as db:
            row = db.execute(
                "SELECT completed, expires, ctime_ns FROM entries WHERE digest = ?", (digest,)
            ).fetchone()
            st = self._file_stat(path)
            now = time()
            if (
                row
                and st
                and st.st_ctime_ns == row["ctime_ns"]
                and now < row["expires"]
                and 0 <= now - row["completed"] < volume.cache_ttl_seconds
            ):
                self.mounts[volume.relative_path] = path
                yield False
                return

            self._remove(db, digest)
            source = directory / volume.relative_path
            if source.exists():
                raise VolumeDownloadFailed("Conflicting cached input destinations")
            try:
                yield True
            except (httpx.HTTPError, httpx.InvalidURL, ValueError) as exc:
                # HTTP exceptions may include signed URLs.
                detail = type(exc).__name__
                if isinstance(exc, httpx.HTTPStatusError):
                    detail = f"HTTP {exc.response.status_code}"
                raise VolumeDownloadFailed("HTTP input download failed", detail) from None
            self._store(db, digest, source, volume.cache_ttl_seconds)
        self.mounts[volume.relative_path] = path

    def _store(self, db: sqlite3.Connection, digest: str, source: Path, ttl: int) -> None:
        path = self.root / digest
        db.execute("INSERT INTO entries VALUES (?, 0, 0, 0, 0)", (digest,))
        try:
            shutil.move(source, path)
            with path.open("rb") as file, open_directory(self.root) as fd:
                os.fsync(file.fileno())
                os.fsync(fd)
            completed = time()
            st = path.stat()
            db.execute(
                "UPDATE entries SET completed=?, expires=?, size=?, ctime_ns=? WHERE digest=?",
                (completed, completed + min(ttl, 10**20), st.st_size, st.st_ctime_ns, digest),
            )
        except BaseException:
            self._remove(db, digest)
            raise

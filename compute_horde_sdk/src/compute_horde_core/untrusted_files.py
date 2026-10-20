"""
Reading and writing files in a directory that an untrusted process has populated.

A job container writes into host directories that are later read with the
privileges of the process collecting the results. Every entry is treated as
hostile: no path component is ever followed as a symlink, only regular files
are opened, and the size reported by the kernel is checked before any content
is read. A file is written there only as a new regular file that replaces
whatever entry had its name. Relative paths chosen by an untrusted party are
accepted only in a single canonical spelling, so a path can never name
anything above the directory it is resolved against.
"""

import contextlib
import dataclasses
import errno
import os
import pathlib
import stat
from collections.abc import Iterator

# O_NONBLOCK and O_NOCTTY: an entry swapped for a FIFO or a terminal between the
# type check and the open neither blocks the open nor becomes the controlling terminal.
_OPEN_FLAGS = {
    stat.S_IFDIR: os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
    stat.S_IFREG: os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK | os.O_NOCTTY,
}
# O_EXCL: an entry planted under the name after it was unlinked fails the open instead of being opened.
_WRITE_FLAGS = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK
_FILE_TYPE_NAMES = {
    stat.S_IFREG: "regular file",
    stat.S_IFDIR: "directory",
    stat.S_IFLNK: "symlink",
    stat.S_IFIFO: "FIFO",
    stat.S_IFCHR: "character device",
    stat.S_IFBLK: "block device",
    stat.S_IFSOCK: "socket",
}
_READ_CHUNK_SIZE = 1 << 20
_FORBIDDEN_COMPONENTS = frozenset({"", ".", ".."})
_MAX_NAME_BYTES = 255
# Every directory level being walked keeps a descriptor open and a generator frame alive.
_MAX_WALK_DEPTH = 100


class UntrustedFileRejected(Exception):
    """A path under an untrusted directory was not opened or read; `reason` says why."""

    def __init__(self, path: str, reason: str) -> None:
        super().__init__(f"{path}: {reason}")
        self.path = path
        self.reason = reason


class InvalidRelativePath(UntrustedFileRejected, ValueError):
    """Also a ValueError, so pydantic validators can raise it as is."""


class SymlinkRejected(UntrustedFileRejected):
    pass


class UnexpectedFileType(UntrustedFileRejected):
    pass


class FileTooLarge(UntrustedFileRejected):
    pass


class FileSizeChanged(UntrustedFileRejected):
    pass


class EntryNotReplaceable(UntrustedFileRejected):
    pass


class DirectoryTooDeep(UntrustedFileRejected):
    pass


@dataclasses.dataclass(frozen=True)
class OpenedFile:
    path: str
    fd: int
    stat: os.stat_result


def split_relative_path(relative_path: str) -> list[str]:
    """
    Components of a relative path in its single accepted spelling, or `InvalidRelativePath`.

    Meant for paths chosen by an untrusted party. Accepted: a path `open_regular_file`
    accepts that contains no backslash and no component longer than 255 bytes in UTF-8.
    Joining the components with '/' gives the input back, so a validated path needs no
    normalization anywhere.
    """
    if "\\" in relative_path:
        raise InvalidRelativePath(relative_path, "must not contain backslashes")
    components = _split_confined(relative_path)
    if any(len(component.encode()) > _MAX_NAME_BYTES for component in components):
        raise InvalidRelativePath(relative_path, f"must not have components longer than {_MAX_NAME_BYTES} bytes")
    return components


@contextlib.contextmanager
def open_directory(path: str | os.PathLike[str]) -> Iterator[int]:
    """Open `path` as a directory descriptor; `path` itself must not be a symlink."""
    with _open_component(None, path, os.fspath(path), stat.S_IFDIR) as (fd, _):
        yield fd


def list_entries(dir_fd: int) -> list[str]:
    """Names of the current entries in the directory, whatever their type."""
    # Rewind first: btrfs otherwise hides entries created after the descriptor was opened.
    os.lseek(dir_fd, 0, os.SEEK_SET)
    return os.listdir(dir_fd)


def walk_regular_files(dir_fd: int) -> Iterator[str]:
    """
    Relative paths of the regular files below the directory `dir_fd`, in no particular order.

    Subdirectories are entered through descriptors opened without following
    symlinks. Symlinks and every other non-regular entry are skipped. Names are
    kept as `os.listdir` returns them, so they may contain backslashes or
    surrogate escapes; `open_regular_file` accepts every path yielded here.
    A directory nested more than 100 levels deep raises `DirectoryTooDeep`.
    """
    yield from _walk(dir_fd, pathlib.PurePosixPath())


@contextlib.contextmanager
def open_regular_file(dir_fd: int, relative_path: str) -> Iterator[OpenedFile]:
    """
    Open `relative_path` beneath the directory `dir_fd` without following any symlink.

    `relative_path` must be non-empty and '/'-separated, without a leading '/', NUL
    bytes or empty, '.' or '..' components, or `InvalidRelativePath` is raised; any
    other character, a backslash included, is part of a name. Each component is
    opened relative to the previous one, so a symlink or an unexpected file type
    anywhere in the path is rejected instead of followed.
    """
    *directories, name = _split_confined(relative_path)
    with contextlib.ExitStack() as stack:
        walked = pathlib.PurePosixPath()
        for directory in directories:
            walked /= directory
            dir_fd, _ = stack.enter_context(_open_component(dir_fd, directory, str(walked), stat.S_IFDIR))
        fd, st = stack.enter_context(_open_component(dir_fd, name, relative_path, stat.S_IFREG))
        yield OpenedFile(path=relative_path, fd=fd, stat=st)


def read_bounded(file: OpenedFile, max_size: int) -> bytes:
    """
    Read the whole content of an opened regular file of at most `max_size` bytes.

    The size reported by fstat is checked before reading and never more than
    `max_size + 1` bytes are read, so an oversized file costs no memory. Content
    that does not match the reported size, because the file is still being
    written to, is rejected.
    """
    expected_size = file.stat.st_size
    if expected_size > max_size:
        raise FileTooLarge(file.path, f"is {expected_size:,} bytes, more than the allowed {max_size:,} bytes")
    content = _read_up_to(file.fd, expected_size + 1)
    if len(content) != expected_size:
        raise FileSizeChanged(file.path, f"read {len(content):,} bytes, expected {expected_size:,} bytes")
    return content


def read_regular_file(dir_fd: int, relative_path: str, max_size: int) -> bytes:
    with open_regular_file(dir_fd, relative_path) as file:
        return read_bounded(file, max_size)


def _read_up_to(fd: int, limit: int) -> bytes:
    chunks: list[bytes] = []
    remaining = limit
    while remaining > 0:
        chunk = os.read(fd, min(remaining, _READ_CHUNK_SIZE))
        if not chunk:
            break
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def _split_confined(relative_path: str) -> list[str]:
    if not relative_path:
        raise InvalidRelativePath(relative_path, "must not be empty")
    if "\0" in relative_path:
        raise InvalidRelativePath(relative_path, "must not contain NUL bytes")
    if relative_path.startswith("/"):
        raise InvalidRelativePath(relative_path, "must be relative, not absolute")
    components = relative_path.split("/")
    if _FORBIDDEN_COMPONENTS.intersection(components):
        raise InvalidRelativePath(relative_path, "must not contain empty, '.' or '..' components or end with '/'")
    return components


def _walk(dir_fd: int, prefix: pathlib.PurePosixPath) -> Iterator[str]:
    for name in list_entries(dir_fd):
        path = prefix / name
        file_type = stat.S_IFMT(os.stat(name, dir_fd=dir_fd, follow_symlinks=False).st_mode)
        if file_type == stat.S_IFREG:
            yield str(path)
        elif file_type == stat.S_IFDIR:
            if len(path.parts) > _MAX_WALK_DEPTH:
                raise DirectoryTooDeep(str(path), f"is nested more than {_MAX_WALK_DEPTH} directories deep")
            with _open_component(dir_fd, name, str(path), stat.S_IFDIR) as (sub_fd, _):
                yield from _walk(sub_fd, path)


@contextlib.contextmanager
def _open_component(
    dir_fd: int | None, name: str | os.PathLike[str], path: str, expected_type: int
) -> Iterator[tuple[int, os.stat_result]]:
    # Check the type before opening: opening a device node can have side effects.
    _check_type(os.stat(name, dir_fd=dir_fd, follow_symlinks=False), path, expected_type)
    try:
        fd = os.open(name, _OPEN_FLAGS[expected_type], dir_fd=dir_fd)
    except OSError as e:
        if e.errno == errno.ELOOP:
            raise SymlinkRejected(path, "is a symlink") from e
        raise
    try:
        # The open descriptor is what gets read, so its type is the one that counts.
        st = os.fstat(fd)
        _check_type(st, path, expected_type)
        yield fd, st
    finally:
        os.close(fd)


def _check_type(st: os.stat_result, path: str, expected_type: int) -> None:
    actual_type = stat.S_IFMT(st.st_mode)
    if actual_type == expected_type:
        return
    if actual_type == stat.S_IFLNK:
        raise SymlinkRejected(path, "is a symlink")
    actual_name = _FILE_TYPE_NAMES.get(actual_type, "file of unknown type")
    raise UnexpectedFileType(path, f"is a {actual_name}, expected a {_FILE_TYPE_NAMES[expected_type]}")


def write_regular_file(dir_fd: int, name: str, data: str | bytes) -> None:
    """
    Create `name` directly beneath the directory `dir_fd` as a new regular file holding `data`.

    `name` must be a single file name; '', '.', '..' and names containing '/' or
    a NUL byte raise `InvalidRelativePath`. Whatever the untrusted process left
    under that name is unlinked first, so a symlink is never followed and a FIFO
    is never opened. A directory cannot be unlinked and is rejected with
    `EntryNotReplaceable`. `str` data is written as UTF-8.
    """
    _check_file_name(name)
    try:
        os.unlink(name, dir_fd=dir_fd)
    except FileNotFoundError:
        pass
    except OSError as e:
        raise EntryNotReplaceable(name, f"cannot be replaced: {e.strerror}") from e
    fd = os.open(name, _WRITE_FLAGS, 0o644, dir_fd=dir_fd)
    try:
        _write_all(fd, data.encode() if isinstance(data, str) else data)
    finally:
        os.close(fd)


def _check_file_name(name: str) -> None:
    if name in ("", ".", "..") or "/" in name or "\0" in name:
        raise InvalidRelativePath(name, "must be a single file name")


def _write_all(fd: int, data: bytes) -> None:
    view = memoryview(data)
    while view:
        view = view[os.write(fd, view) :]

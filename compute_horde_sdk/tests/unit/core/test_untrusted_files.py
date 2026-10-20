import os
import pathlib
import socket
import stat

import pytest

from compute_horde_core.untrusted_files import (
    DirectoryTooDeep,
    EntryNotReplaceable,
    FileSizeChanged,
    FileTooLarge,
    InvalidRelativePath,
    SymlinkRejected,
    UnexpectedFileType,
    list_entries,
    open_directory,
    open_regular_file,
    read_bounded,
    read_regular_file,
    split_relative_path,
    walk_regular_files,
    write_regular_file,
)

MAX_SIZE = 1000


@pytest.fixture
def secret_file(tmp_path):
    path = tmp_path / "secret"
    path.write_bytes(b"EXECUTOR_TOKEN=do-not-leak")
    return path


@pytest.fixture
def directory(tmp_path):
    path = tmp_path / "untrusted"
    path.mkdir()
    return path


@pytest.fixture
def dir_fd(directory):
    with open_directory(directory) as fd:
        yield fd


@pytest.fixture
def unix_socket(directory):
    with socket.socket(socket.AF_UNIX) as sock:
        sock.bind(str(directory / "socket"))
        yield


def test_regular_file_is_read(directory, dir_fd):
    (directory / "artifact.txt").write_bytes(b"artifact content")

    assert read_regular_file(dir_fd, "artifact.txt", max_size=MAX_SIZE) == b"artifact content"


def test_empty_file_is_read(directory, dir_fd):
    (directory / "empty").write_bytes(b"")

    assert read_regular_file(dir_fd, "empty", max_size=MAX_SIZE) == b""


def test_file_exactly_at_limit_is_read(directory, dir_fd):
    (directory / "at-limit").write_bytes(b"x" * MAX_SIZE)

    assert read_regular_file(dir_fd, "at-limit", max_size=MAX_SIZE) == b"x" * MAX_SIZE


def test_file_over_limit_is_rejected(directory, dir_fd):
    (directory / "over-limit").write_bytes(b"x" * (MAX_SIZE + 1))

    with pytest.raises(FileTooLarge):
        read_regular_file(dir_fd, "over-limit", max_size=MAX_SIZE)


def test_sparse_file_over_limit_is_rejected(directory, dir_fd):
    with open(directory / "sparse", "wb") as f:
        f.truncate(8 * 1024 * 1024)

    with pytest.raises(FileTooLarge):
        read_regular_file(dir_fd, "sparse", max_size=MAX_SIZE)


def test_symlink_to_file_outside_directory_is_rejected(directory, dir_fd, secret_file):
    os.symlink(secret_file, directory / "leak")

    with pytest.raises(SymlinkRejected):
        read_regular_file(dir_fd, "leak", max_size=MAX_SIZE)


def test_symlink_to_proc_environ_is_rejected(directory, dir_fd):
    os.symlink("/proc/self/environ", directory / "leak")

    with pytest.raises(SymlinkRejected):
        read_regular_file(dir_fd, "leak", max_size=MAX_SIZE)


def test_symlink_to_file_inside_directory_is_rejected(directory, dir_fd):
    (directory / "artifact.txt").write_bytes(b"artifact content")
    os.symlink("artifact.txt", directory / "link")

    with pytest.raises(SymlinkRejected):
        read_regular_file(dir_fd, "link", max_size=MAX_SIZE)


def test_dangling_symlink_is_rejected(directory, dir_fd):
    os.symlink("missing", directory / "dangling")

    with pytest.raises(SymlinkRejected):
        read_regular_file(dir_fd, "dangling", max_size=MAX_SIZE)


def test_directory_entry_is_rejected(directory, dir_fd):
    (directory / "subdir").mkdir()

    with pytest.raises(UnexpectedFileType, match="is a directory"):
        read_regular_file(dir_fd, "subdir", max_size=MAX_SIZE)


def test_fifo_is_rejected_without_blocking(directory, dir_fd):
    os.mkfifo(directory / "fifo")

    with pytest.raises(UnexpectedFileType, match="is a FIFO"):
        read_regular_file(dir_fd, "fifo", max_size=MAX_SIZE)


def test_socket_is_rejected(dir_fd, unix_socket):
    with pytest.raises(UnexpectedFileType, match="is a socket"):
        read_regular_file(dir_fd, "socket", max_size=MAX_SIZE)


@pytest.mark.skipif(os.geteuid() != 0, reason="creating device nodes requires root")
def test_device_node_is_rejected(directory, dir_fd):
    os.mknod(directory / "null", mode=0o600 | os.makedev(1, 3))

    with pytest.raises(UnexpectedFileType, match="is a character device"):
        read_regular_file(dir_fd, "null", max_size=MAX_SIZE)


def test_missing_entry_raises_os_error(dir_fd):
    with pytest.raises(FileNotFoundError):
        read_regular_file(dir_fd, "missing", max_size=MAX_SIZE)


def test_multi_component_path_is_read(directory, dir_fd):
    (directory / "a" / "b").mkdir(parents=True)
    (directory / "a" / "b" / "file.txt").write_bytes(b"nested")

    assert read_regular_file(dir_fd, "a/b/file.txt", max_size=MAX_SIZE) == b"nested"


def test_symlinked_intermediate_directory_is_rejected(directory, dir_fd, secret_file):
    os.symlink(secret_file.parent, directory / "escape")

    with pytest.raises(SymlinkRejected, match="^escape: "):
        read_regular_file(dir_fd, f"escape/{secret_file.name}", max_size=MAX_SIZE)


def test_regular_file_as_intermediate_component_is_rejected(directory, dir_fd):
    (directory / "file").write_bytes(b"not a directory")

    with pytest.raises(UnexpectedFileType, match="^file: is a regular file, expected a directory"):
        read_regular_file(dir_fd, "file/child", max_size=MAX_SIZE)


MALFORMED_RELATIVE_PATHS = [
    "",
    ".",
    "/",
    "/etc/passwd",
    "//etc/passwd",
    "../secret",
    "a/../../secret",
    "./file",
    "a/./file",
    "a//file",
    "dir/",
    "a\0file",
]


@pytest.mark.parametrize("relative_path", MALFORMED_RELATIVE_PATHS)
def test_malformed_relative_path_is_rejected(dir_fd, relative_path):
    with pytest.raises(InvalidRelativePath):
        read_regular_file(dir_fd, relative_path, max_size=MAX_SIZE)


@pytest.mark.parametrize(
    "relative_path",
    [
        *MALFORMED_RELATIVE_PATHS,
        "a\\file",
        pytest.param("x" * 256, id="256-byte-name"),
        pytest.param("dir/" + "é" * 128, id="256-byte-utf8-name"),
    ],
)
def test_invalid_relative_path_is_a_value_error(relative_path):
    with pytest.raises(ValueError):
        split_relative_path(relative_path)


def test_backslash_is_part_of_a_name(directory, dir_fd):
    (directory / "a\\file").write_bytes(b"backslash")

    assert read_regular_file(dir_fd, "a\\file", max_size=MAX_SIZE) == b"backslash"


@pytest.mark.parametrize(
    "relative_path,components",
    [
        ("file", ["file"]),
        ("a/b/file.txt", ["a", "b", "file.txt"]),
        (".hidden", [".hidden"]),
        ("a b/c", ["a b", "c"]),
        pytest.param("x" * 255, ["x" * 255], id="255-byte-name"),
        pytest.param("é" * 127, ["é" * 127], id="254-byte-utf8-name"),
    ],
)
def test_split_relative_path(relative_path, components):
    assert split_relative_path(relative_path) == components


def test_walk_regular_files_skips_everything_but_regular_files(directory, dir_fd, secret_file):
    (directory / "a.txt").write_bytes(b"")
    (directory / "sub" / "deeper").mkdir(parents=True)
    (directory / "sub" / "b.txt").write_bytes(b"")
    (directory / "sub" / "deeper" / "c.txt").write_bytes(b"")
    (directory / "sub" / "back\\slash").write_bytes(b"")
    (directory / "empty-dir").mkdir()
    os.symlink(secret_file, directory / "leak")
    os.symlink(secret_file.parent, directory / "escape")
    os.symlink("a.txt", directory / "sub" / "link-inside")
    os.mkfifo(directory / "fifo")

    assert sorted(walk_regular_files(dir_fd)) == ["a.txt", "sub/b.txt", "sub/back\\slash", "sub/deeper/c.txt"]


def test_walk_regular_files_enters_directories_100_levels_deep(directory, dir_fd):
    deepest = directory.joinpath(*["d"] * 100)
    deepest.mkdir(parents=True)
    (deepest / "file").write_bytes(b"")

    assert list(walk_regular_files(dir_fd)) == ["d/" * 100 + "file"]


def test_walk_regular_files_rejects_directory_101_levels_deep(directory, dir_fd):
    directory.joinpath(*["d"] * 101).mkdir(parents=True)

    with pytest.raises(DirectoryTooDeep, match=f"^{'d/' * 100}d: is nested more than 100 directories deep$"):
        list(walk_regular_files(dir_fd))


def test_file_grown_after_open_is_rejected(directory, dir_fd):
    path = directory / "growing"
    path.write_bytes(b"x" * 10)

    with open_regular_file(dir_fd, "growing") as file:
        with path.open("ab") as f:
            f.write(b"y" * 10)
        with pytest.raises(FileSizeChanged):
            read_bounded(file, max_size=MAX_SIZE)


def test_file_truncated_after_open_is_rejected(directory, dir_fd):
    path = directory / "shrinking"
    path.write_bytes(b"x" * 10)

    with open_regular_file(dir_fd, "shrinking") as file:
        path.write_bytes(b"x" * 5)
        with pytest.raises(FileSizeChanged):
            read_bounded(file, max_size=MAX_SIZE)


def test_open_directory_rejects_symlink(tmp_path, directory):
    os.symlink(directory, tmp_path / "link-to-dir")

    with pytest.raises(SymlinkRejected):
        with open_directory(tmp_path / "link-to-dir"):
            pass


def test_open_directory_rejects_regular_file(tmp_path):
    (tmp_path / "file").write_bytes(b"")

    with pytest.raises(UnexpectedFileType, match="is a regular file, expected a directory"):
        with open_directory(tmp_path / "file"):
            pass


def test_list_entries_lists_everything_without_following(directory, dir_fd, secret_file):
    (directory / "artifact.txt").write_bytes(b"")
    os.symlink(secret_file, directory / "leak")
    os.mkfifo(directory / "fifo")

    assert sorted(list_entries(dir_fd)) == ["artifact.txt", "fifo", "leak"]


@pytest.mark.parametrize("data", ["job output", b"job output"])
def test_write_regular_file_creates_regular_file(directory, dir_fd, data):
    write_regular_file(dir_fd, "stdout.txt", data)

    path = directory / "stdout.txt"
    assert stat.S_ISREG(path.lstat().st_mode)
    assert path.read_bytes() == b"job output"


def test_write_regular_file_replaces_existing_file(directory, dir_fd):
    (directory / "stdout.txt").write_bytes(b"left by the job")

    write_regular_file(dir_fd, "stdout.txt", "job output")

    assert (directory / "stdout.txt").read_bytes() == b"job output"


def test_write_regular_file_replaces_symlink_without_following(directory, dir_fd, secret_file):
    os.symlink(secret_file, directory / "stdout.txt")

    write_regular_file(dir_fd, "stdout.txt", "job output")

    path = directory / "stdout.txt"
    assert stat.S_ISREG(path.lstat().st_mode)
    assert path.read_bytes() == b"job output"
    assert secret_file.read_bytes() == b"EXECUTOR_TOKEN=do-not-leak"


def test_write_regular_file_replaces_fifo_without_blocking(directory, dir_fd):
    os.mkfifo(directory / "stdout.txt")

    write_regular_file(dir_fd, "stdout.txt", "job output")

    path = directory / "stdout.txt"
    assert stat.S_ISREG(path.lstat().st_mode)
    assert path.read_bytes() == b"job output"


def test_write_regular_file_rejects_directory(directory, dir_fd):
    (directory / "stdout.txt").mkdir()

    with pytest.raises(EntryNotReplaceable, match="^stdout.txt: cannot be replaced"):
        write_regular_file(dir_fd, "stdout.txt", "job output")

    assert (directory / "stdout.txt").is_dir()


@pytest.fixture
def outside_directory(tmp_path):
    path = tmp_path / "outside"
    path.mkdir()
    (path / "x").write_bytes(b"outside content")
    return path


@pytest.fixture
def symlink_to_outside_directory(directory, outside_directory):
    os.symlink(outside_directory, directory / "sub")


def tree_contents(root: pathlib.Path) -> dict[str, bytes | str]:
    """Regular files mapped to their content and symlinks to their target, keyed by path relative to `root`."""
    return {
        str(path.relative_to(root)): os.readlink(path) if path.is_symlink() else path.read_bytes()
        for path in root.rglob("*")
        if path.is_symlink() or path.is_file()
    }


@pytest.mark.parametrize("name", ["sub/x", "../x", "", ".", "..", "x/", "x\0"])
def test_write_regular_file_rejects_anything_but_a_single_file_name(
    tmp_path, dir_fd, outside_directory, symlink_to_outside_directory, name
):
    with pytest.raises(InvalidRelativePath, match="must be a single file name$"):
        write_regular_file(dir_fd, name, "job output")

    assert tree_contents(tmp_path) == {
        "outside/x": b"outside content",
        "untrusted/sub": str(outside_directory),
    }

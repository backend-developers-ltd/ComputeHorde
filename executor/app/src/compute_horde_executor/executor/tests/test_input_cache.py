import hashlib
import shutil
import subprocess
import sys
import threading
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock

import pytest
from asgiref.sync import async_to_sync
from compute_horde.job_errors import JobError
from compute_horde.protocol_messages import V0InitialJobRequest, V0JobRequest
from compute_horde_core.volume import (
    SingleFileVolume,
    SingleFileVolumeDownloader,
    VolumeDownloadFailed,
)
from compute_horde_sdk.v1 import HTTPInputVolume
from pydantic import ValidationError

from compute_horde_executor.executor import input_cache
from compute_horde_executor.executor.input_cache import InputCache
from compute_horde_executor.executor.job_runner import DefaultJobRunner


@pytest.fixture
def origin():
    requests = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            requests.append(("GET", self.path))
            body = self.path.partition("?")[0].encode()
            self.send_response(404 if self.path.startswith("/missing") else 200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_HEAD(self):
            requests.append(("HEAD", self.path))
            self.send_error(405)

        def log_message(self, *args):
            pass

    with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        yield f"http://127.0.0.1:{server.server_port}", requests
        server.shutdown()
        thread.join()


def download(root, url, key="k", ttl=3600, budget=100, limit=100):
    cache = InputCache(budget, root)
    volume = SingleFileVolume(url=url, relative_path="file", cache_key=key, cache_ttl_seconds=ttl)
    child = SingleFileVolumeDownloader(volume)
    child.max_size_bytes = limit
    child.max_retries = 1
    child.cache_hook = cache.prepare
    with TemporaryDirectory(dir=root.parent) as directory:
        async_to_sync(child.download)(Path(directory))
    return cache.mounts["file"]


@pytest.mark.parametrize(
    "ttl", [{}, *({"cache_ttl_seconds": t} for t in [None, 0, -1, True, "60", 1.0, [], {}])]
)
def test_disabled_wire_and_download(runner, origin, ttl):
    base, requests = origin
    params = {"url": base + "/plain", "cache_key": ["invalid"], **ttl}
    sdk = HTTPInputVolume(**params)
    expected_sdk = {"url": params["url"]}
    expected = {**expected_sdk, "relative_path": "plain", "volume_type": "single_file"}
    wire = SingleFileVolume(**params, relative_path="plain")
    assert [
        sdk.model_dump(),
        sdk.to_compute_horde_volume("/volume/plain").model_dump(),
        wire.model_dump(),
    ] == [
        expected_sdk,
        expected,
        expected,
    ]
    prepare(runner, [wire.model_dump()])
    assert not runner.input_cache.root.exists()
    assert ((runner.volume_mount_dir / "plain").read_bytes(), requests) == (
        b"/plain",
        [("GET", "/plain")],
    )


@pytest.mark.parametrize("key", [{}, {"cache_key": ""}, {"cache_key": 1}, {"cache_key": None}])
def test_positive_ttl_requires_key(key):
    for model, extra in [(HTTPInputVolume, {}), (SingleFileVolume, {"relative_path": "file"})]:
        with pytest.raises(ValidationError, match="cache_key must be a nonempty string"):
            model(url="http://example.org", cache_ttl_seconds=3600, **extra, **key)


def test_cache_uses_default_permissions(tmp_path, origin):
    base, _ = origin
    directory = tmp_path / "ordinary"
    directory.mkdir()
    file = directory / "file"
    file.touch()
    root = tmp_path / "cache"
    source = download(root, base + "/data")
    assert (root.stat().st_mode, source.stat().st_mode) == (
        directory.stat().st_mode,
        file.stat().st_mode,
    )


def test_identity_and_integrity(tmp_path, origin):
    base, requests = origin
    root = tmp_path / "cache"
    key = "../Ä/Img"
    source = download(root, base + "/a?sig=old", key)
    assert source.name == hashlib.sha256(key.encode()).hexdigest()
    assert download(root, base + "/b?sig=new", key) == source
    assert download(root, base + "/a?sig=old", key.lower()) != source
    source.write_bytes(b"xx")
    assert download(root, base + "/b", key).read_bytes() == b"/b"
    shutil.rmtree(root)
    assert download(root, base + "/b", key).read_bytes() == b"/b"
    assert requests == [("GET", "/a?sig=old"), ("GET", "/a?sig=old"), ("GET", "/b"), ("GET", "/b")]


@pytest.mark.parametrize(("written_ttl", "requested_ttl"), [(10, 100), (100, 10)])
def test_expiry_and_failed_refresh(tmp_path, origin, monkeypatch, written_ttl, requested_ttl):
    base, requests = origin
    root = tmp_path / "cache"
    clock = Mock(return_value=100)
    monkeypatch.setattr(input_cache, "time", clock)
    source = download(root, base + "/a", ttl=written_ttl)
    clock.return_value = 109
    assert download(root, base + "/hit", ttl=requested_ttl) == source
    clock.return_value = 110
    with pytest.raises(VolumeDownloadFailed, match="HTTP input download failed"):
        download(root, base + "/missing?signature=secret", ttl=requested_ttl)
    assert not source.exists()
    assert download(root, base + "/b").read_bytes() == b"/b"
    assert requests == [("GET", "/a"), ("GET", "/missing?signature=secret"), ("GET", "/b")]


def test_budget_only_applies_during_cleanup(tmp_path, origin):
    base, requests = origin
    root = tmp_path / "cache"
    first = download(root, base + "/a", budget=3)
    second = download(root, base + "/b", key="revision-2", budget=3)
    cache = InputCache(3, root)
    assert [first.read_bytes(), second.read_bytes()] == [b"/a", b"/b"]
    cache.sweep()
    assert (first.exists(), second.read_bytes()) == (False, b"/b")
    large = download(root, base + "/large", budget=3)
    assert download(root, base + "/hit", budget=3) == large
    assert large.read_bytes() == b"/large"
    cache.sweep()
    assert not large.exists()
    with pytest.raises(VolumeDownloadFailed, match="too large"):
        download(root, base + "/large", limit=2)
    assert requests == [("GET", p) for p in ["/a", "/b", "/large", "/large"]]


def test_sweep(tmp_path, origin, monkeypatch):
    base, _ = origin
    clock = Mock(return_value=1000)
    monkeypatch.setattr(input_cache, "time", clock)
    cache = InputCache(100, tmp_path / "cache")
    source = download(cache.root, base + "/a", ttl=1)
    cache.sweep()
    clock.return_value = 1002
    partial = cache.root / ".partial"
    partial.touch()
    cache.sweep()
    assert (source.exists(), partial.exists()) == (True, True)
    clock.return_value = 1300
    cache.sweep()
    assert (source.exists(), partial.exists()) == (False, False)


def test_unsafe_cache_destinations(tmp_path, origin):
    base, requests = origin
    root = tmp_path / "cache"
    (tmp_path / "link").symlink_to(tmp_path.parent)
    for dest in ["../escape", "/absolute", "link/through", "link", "a//b"]:
        volume = SingleFileVolume(
            url=base, relative_path=dest, cache_key="k", cache_ttl_seconds=3600
        )
        with (
            pytest.raises(VolumeDownloadFailed, match="path"),
            InputCache(100, root).prepare(volume, tmp_path),
        ):
            pytest.fail("Invalid destination reached the downloader")
    assert requests == []


@pytest.fixture
def new_runner(tmp_path):
    jobs = []

    def create():
        job = DefaultJobRunner()
        job.volume_mount_dir.mkdir()
        job.input_cache = InputCache(100, tmp_path / "cache")
        jobs.append(job)
        return job

    yield create
    for job in jobs:
        shutil.rmtree(job.temp_dir)


@pytest.fixture
def runner(new_runner):
    return new_runner()


def prepare(runner, volumes, validator="validator-A"):
    job_id = str(uuid.uuid4())
    executor_class = "spin_up-4min.gpu-24gb"
    runner.initial_job_request = V0InitialJobRequest.model_validate(
        {
            "job_uuid": job_id,
            "executor_class": executor_class,
            "timeout_seconds": 30,
            "job_started_receipt_payload": {
                "job_uuid": job_id,
                "miner_hotkey": "miner",
                "validator_hotkey": validator,
                "timestamp": "2025-01-01T00:00:00Z",
                "executor_class": executor_class,
                "is_organic": True,
                "ttl": 5,
            },
            "job_started_receipt_signature": "fixture",
        }
    )
    runner.full_job_request = V0JobRequest.model_validate(
        {
            "job_uuid": job_id,
            "executor_class": executor_class,
            "docker_image": "busybox:1.37",
            "docker_run_options_preset": "none",
            "docker_run_cmd": [],
            "volume": {"volume_type": "multi_volume", "volumes": volumes},
        }
    )
    async_to_sync(runner.download_volume)()


def test_multi_volume_mount_targets(runner, origin):
    base, requests = origin
    cached = HTTPInputVolume(url=base + "/data", cache_key="data", cache_ttl_seconds=3600)
    plain = HTTPInputVolume(url=base + "/plain")
    volumes = [
        v.to_compute_horde_volume("/volume/d/" + name).model_dump(mode="json")
        for v, name in [(cached, "data"), (plain, "plain"), (cached, "alias")]
    ]
    prepare(runner, volumes)
    contents = {p.name: p.read_bytes() for p in (runner.volume_mount_dir / "d").iterdir()}
    assert contents == {"data": b"", "plain": b"/plain", "alias": b""}
    assert runner.input_cache.mounts["d/data"] == runner.input_cache.mounts["d/alias"]
    assert requests == [("GET", "/data"), ("GET", "/plain")]
    with pytest.raises(JobError, match="Conflicting"):
        prepare(runner, [volumes[0], volumes[0]])
    with pytest.raises(FileExistsError):
        prepare(runner, [volumes[0], dict(volumes[1], relative_path="d/data")])


def test_restart_during_transfer(tmp_path, origin):
    root = tmp_path / "cache"
    sent = threading.Event()
    release = threading.Event()

    class InterruptedOrigin(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Length", "2000000")
            self.end_headers()
            self.wfile.write(b"X" * 1000000)
            sent.set()
            release.wait(20)

        def log_message(self, *args):
            pass

    with ThreadingHTTPServer(("127.0.0.1", 0), InterruptedOrigin) as server:
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        url = f"http://127.0.0.1:{server.server_port}/interrupted"
        script = (
            "from pathlib import Path; from compute_horde_executor.executor.tests.test_input_cache import download; import sys; "
            "download(Path(sys.argv[1]), sys.argv[2], limit=2000000, budget=2000000)"
        )
        process = subprocess.Popen(
            [sys.executable, "-c", script, str(root), url], cwd=Path(__file__).resolve().parents[3]
        )
        try:
            assert sent.wait(20)
            assert list(root.iterdir()) == [root / "index.sqlite"]
        finally:
            process.kill()
            process.wait()
            release.set()
            server.shutdown()
            thread.join()

    base, requests = origin
    assert download(root, base + "/complete").read_bytes() == b"/complete"
    assert requests == [("GET", "/complete")]


def test_docker_file_mounts(new_runner, origin):
    if (
        not shutil.which("docker")
        or subprocess.run(["docker", "info"], capture_output=True).returncode
    ):
        pytest.skip("Docker daemon unavailable")
    base, requests = origin
    command = [
        "sh",
        "-ec",
        """
        cd /volume/inputs
        test "$(cat image)" = /image
        test "$(cat alias)" = /image
        test "$(cat image.idx)" = /image.idx
        test "$(cat plain)" = /plain
        for f in image image.idx; do
            if (echo changed > "$f") 2>/dev/null; then exit 1; fi
        done
        echo changed > plain
        test "$(cat plain)" = changed
        test ! -e /tmp/executor-cache
    """,
    ]

    @async_to_sync
    async def execute(job):
        async with job.start_job():
            pass

    for validator in ["validator-A", "validator-B"]:
        job = new_runner()
        volumes = [
            HTTPInputVolume(
                url=f"{base}/{name}?signature={validator}",
                cache_key=name,
                cache_ttl_seconds=3600 if name != "plain" else None,
            )
            .to_compute_horde_volume(f"/volume/inputs/{name}")
            .model_dump()
            for name in ["image", "image.idx", "plain"]
        ]
        volumes.append(dict(volumes[0], relative_path="inputs/alias"))
        prepare(job, volumes, validator)
        job.full_job_request.docker_run_cmd = command
        execute(job)
        assert job.execution_result.return_code == 0
    assert requests == [
        ("GET", "/image?signature=validator-A"),
        ("GET", "/image.idx?signature=validator-A"),
        ("GET", "/plain?signature=validator-A"),
        ("GET", "/plain?signature=validator-B"),
    ]


def test_job_inputs_can_exceed_budget_until_cleanup(runner, origin):
    base, _ = origin
    runner.input_cache.budget = 3
    volumes = [
        HTTPInputVolume(url=base + name, cache_key=name, cache_ttl_seconds=60)
        .to_compute_horde_volume("/volume" + name)
        .model_dump()
        for name in ["/a", "/b"]
    ]
    prepare(runner, volumes)
    mounts = runner.input_cache.mounts
    assert {name: path.read_bytes() for name, path in mounts.items()} == {"a": b"/a", "b": b"/b"}
    async_to_sync(runner.sweep_input_cache)()
    assert {name for name, path in mounts.items() if path.exists()} == {"b"}

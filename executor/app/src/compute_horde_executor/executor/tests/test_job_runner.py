import base64
import logging
import os
import shutil
import stat
import uuid

import pytest
from asgiref.sync import async_to_sync
from compute_horde.job_errors import JobError
from compute_horde.protocol_consts import JobFailureReason
from compute_horde.protocol_messages import V0InitialJobRequest, V0JobRequest
from compute_horde_core import untrusted_files
from compute_horde_core.output_upload import SingleFilePutUpload

from compute_horde_executor.executor import job_runner
from compute_horde_executor.executor.job_runner import MAX_ARTIFACT_SIZE, DefaultJobRunner
from compute_horde_executor.executor.miner_client import ExecutionResult, JobResult

JOB_UUID = str(uuid.uuid4())
EXECUTOR_CLASS = "spin_up-4min.gpu-24gb"
DOCKER_IMAGE = "backenddevelopersltd/compute-horde-job-echo:v0-latest"


@pytest.fixture
def runner():
    runner = DefaultJobRunner()
    runner.initial_job_request = V0InitialJobRequest.model_validate(
        {
            "job_uuid": JOB_UUID,
            "executor_class": EXECUTOR_CLASS,
            "docker_image": DOCKER_IMAGE,
            "timeout_seconds": 10,
            "job_started_receipt_payload": {
                "job_uuid": JOB_UUID,
                "miner_hotkey": "miner_hotkey",
                "validator_hotkey": "validator_hotkey",
                "timestamp": "2025-01-01T00:00:00+00:00",
                "executor_class": EXECUTOR_CLASS,
                "is_organic": True,
                "ttl": 5,
            },
            "job_started_receipt_signature": "blah",
        }
    )
    runner.full_job_request = V0JobRequest(
        job_uuid=JOB_UUID,
        executor_class=EXECUTOR_CLASS,
        docker_image=DOCKER_IMAGE,
        docker_run_options_preset="none",
        docker_run_cmd=[],
        artifacts_dir="/artifacts",
    )
    runner.execution_result = ExecutionResult(
        timed_out=False, return_code=0, stdout="job stdout", stderr="job stderr"
    )
    runner.output_volume_mount_dir.mkdir()
    runner.artifacts_mount_dir.mkdir()
    yield runner
    shutil.rmtree(runner.temp_dir)


def job_runner_errors(caplog) -> list[str]:
    return [
        record.getMessage()
        for record in caplog.records
        if record.name == job_runner.logger.name and record.levelno == logging.ERROR
    ]


@pytest.fixture
def secret_file(tmp_path):
    path = tmp_path / "secret"
    path.write_bytes(b"EXECUTOR_TOKEN=do-not-leak")
    return path


@pytest.fixture
def hostile_artifacts(runner, secret_file):
    artifacts_dir = runner.artifacts_mount_dir
    (artifacts_dir / "normal.txt").write_bytes(b"artifact content")
    os.symlink(secret_file, artifacts_dir / "secret-link")
    os.symlink("/proc/self/environ", artifacts_dir / "environ-link")
    os.symlink("missing", artifacts_dir / "dangling-link")
    os.mkfifo(artifacts_dir / "fifo")
    (artifacts_dir / "subdir").mkdir()
    (artifacts_dir / "too-large.bin").write_bytes(b"x" * MAX_ARTIFACT_SIZE)
    with open(artifacts_dir / "huge-sparse.bin", "wb") as f:
        f.truncate(8 * 1024 * 1024)


def test_upload_results_collects_only_regular_files(runner, hostile_artifacts, caplog):
    result = async_to_sync(runner.upload_results)()

    assert result == JobResult(
        exit_status=0,
        timeout=False,
        stdout="job stdout",
        stderr="job stderr",
        artifacts={"/artifacts/normal.txt": base64.b64encode(b"artifact content").decode()},
        upload_results={},
    )
    assert job_runner_errors(caplog) == [
        "Artifact skipped: dangling-link: is a symlink",
        "Artifact skipped: environ-link: is a symlink",
        "Artifact skipped: fifo: is a FIFO, expected a regular file",
        "Artifact skipped: huge-sparse.bin: is 8,388,608 bytes, more than the allowed 999,999 bytes",
        "Artifact skipped: secret-link: is a symlink",
        "Artifact skipped: subdir: is a directory, expected a regular file",
        "Artifact skipped: too-large.bin: is 1,000,000 bytes, more than the allowed 999,999 bytes",
    ]


@pytest.fixture
def artifact_with_backslash_in_name(runner):
    (runner.artifacts_mount_dir / "a\\b.txt").write_bytes(b"artifact content")


def test_upload_results_collects_artifact_with_backslash_in_name(
    runner, artifact_with_backslash_in_name
):
    result = async_to_sync(runner.upload_results)()

    assert result == JobResult(
        exit_status=0,
        timeout=False,
        stdout="job stdout",
        stderr="job stderr",
        artifacts={"/artifacts/a\\b.txt": base64.b64encode(b"artifact content").decode()},
        upload_results={},
    )


@pytest.fixture
def symlinked_stdout(runner, secret_file):
    os.symlink(secret_file, runner.output_volume_mount_dir / "stdout.txt")


def test_upload_results_writes_stdout_without_following_symlink(
    runner, secret_file, symlinked_stdout
):
    async_to_sync(runner.upload_results)()

    stdout_path = runner.output_volume_mount_dir / "stdout.txt"
    assert stat.S_ISREG(stdout_path.lstat().st_mode)
    assert stdout_path.read_text() == "job stdout"
    assert secret_file.read_bytes() == b"EXECUTOR_TOKEN=do-not-leak"


def test_upload_results_fails_when_stream_file_cannot_be_replaced(runner):
    (runner.output_volume_mount_dir / "stdout.txt").mkdir()

    with pytest.raises(JobError, match="stdout.txt: cannot be replaced") as exc_info:
        async_to_sync(runner.upload_results)()

    assert exc_info.value.reason == JobFailureReason.UPLOAD_FAILED


@pytest.fixture
def artifacts_over_budget(runner, monkeypatch):
    monkeypatch.setattr(job_runner, "MAX_ARTIFACTS_TOTAL_SIZE", 10)
    for name, size in [("a.bin", 4), ("b.bin", 4), ("c.bin", 4), ("d.bin", 2)]:
        (runner.artifacts_mount_dir / name).write_bytes(b"x" * size)


def test_upload_results_skips_artifacts_that_do_not_fit_in_the_total_size_budget(
    runner, artifacts_over_budget, caplog
):
    result = async_to_sync(runner.upload_results)()

    assert result == JobResult(
        exit_status=0,
        timeout=False,
        stdout="job stdout",
        stderr="job stderr",
        artifacts={
            "/artifacts/a.bin": base64.b64encode(b"xxxx").decode(),
            "/artifacts/b.bin": base64.b64encode(b"xxxx").decode(),
            "/artifacts/d.bin": base64.b64encode(b"xx").decode(),
        },
        upload_results={},
    )
    assert job_runner_errors(caplog) == [
        "Artifact skipped: c.bin: would exceed the 10 bytes allowed for all artifacts together"
    ]


@pytest.fixture
def artifacts_over_count(runner, monkeypatch):
    monkeypatch.setattr(job_runner, "MAX_ARTIFACTS_COUNT", 2)
    for name in ["c.txt", "a.txt", "b.txt"]:
        (runner.artifacts_mount_dir / name).write_bytes(b"")


def test_upload_results_collects_only_the_first_artifacts_in_name_order(
    runner, artifacts_over_count, caplog
):
    result = async_to_sync(runner.upload_results)()

    assert result == JobResult(
        exit_status=0,
        timeout=False,
        stdout="job stdout",
        stderr="job stderr",
        artifacts={"/artifacts/a.txt": "", "/artifacts/b.txt": ""},
        upload_results={},
    )
    assert job_runner_errors(caplog) == [
        "Artifacts skipped: only the first 2 of 3 entries in name order are collected"
    ]


@pytest.fixture
def artifact_over_remaining_budget(runner, monkeypatch):
    monkeypatch.setattr(job_runner, "MAX_ARTIFACTS_TOTAL_SIZE", 10)
    (runner.artifacts_mount_dir / "a.bin").write_bytes(b"x" * 10)
    (runner.artifacts_mount_dir / "b.bin").write_bytes(b"x" * 11)


@pytest.fixture
def read_chunk_sizes(monkeypatch):
    sizes = []
    original_read = untrusted_files.os.read

    def recording_read(fd, length):
        chunk = original_read(fd, length)
        sizes.append(len(chunk))
        return chunk

    monkeypatch.setattr(untrusted_files.os, "read", recording_read)
    return sizes


def test_upload_results_does_not_read_an_artifact_over_the_remaining_budget(
    runner, artifact_over_remaining_budget, read_chunk_sizes
):
    result = async_to_sync(runner.upload_results)()

    assert result == JobResult(
        exit_status=0,
        timeout=False,
        stdout="job stdout",
        stderr="job stderr",
        artifacts={"/artifacts/a.bin": base64.b64encode(b"x" * 10).decode()},
        upload_results={},
    )
    assert read_chunk_sizes == [10, 0]


@pytest.fixture
def symlink_output_upload(runner, secret_file):
    os.symlink(secret_file, runner.output_volume_mount_dir / "leak")
    runner.full_job_request.output_upload = SingleFilePutUpload(
        url="https://example.com/upload", relative_path="leak"
    )


def test_upload_results_does_not_follow_symlink_in_output_dir(
    runner, symlink_output_upload, httpx_mock
):
    with pytest.raises(JobError) as exc_info:
        async_to_sync(runner.upload_results)()

    assert exc_info.value.reason == JobFailureReason.UPLOAD_FAILED
    assert httpx_mock.get_requests() == []

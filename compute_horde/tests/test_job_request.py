import base64
import uuid

from compute_horde_core.signature import BittensorWalletSigner, BittensorWalletVerifier, Signature
from compute_horde_core.volume import VolumeType, ZipUrlVolume

from compute_horde.executor_class import DEFAULT_EXECUTOR_CLASS
from compute_horde.fv_protocol.facilitator_requests import V2JobRequest


def test_signed_job_roundtrip(signature_wallet):
    volume = ZipUrlVolume(
        volume_type=VolumeType.zip_url,
        contents="https://example.com/input.zip",
        relative_path="input",
    )
    job = V2JobRequest(
        uuid=str(uuid.uuid4()),
        executor_class=DEFAULT_EXECUTOR_CLASS,
        docker_image="hello-world",
        args=["--verbose", "--dry-run"],
        env={"CUDA": "1"},
        use_gpu=False,
        volume=volume,
        output_upload=None,
        download_time_limit=1,
        execution_time_limit=1,
        upload_time_limit=1,
        streaming_start_time_limit=1,
    )

    signer = BittensorWalletSigner(signature_wallet)
    payload = job.json_for_signing()
    raw_signature = signer.sign(payload)

    job.signature = Signature(
        signature_type=raw_signature.signature_type,
        signatory=raw_signature.signatory,
        timestamp_ns=raw_signature.timestamp_ns,
        signature=base64.b64encode(raw_signature.signature),
    )

    job_json = job.model_dump_json()
    deserialized_job = V2JobRequest.model_validate_json(job_json)

    assert deserialized_job.signature is not None
    deserialized_raw_signature = Signature(
        signature_type=deserialized_job.signature.signature_type,
        signatory=deserialized_job.signature.signatory,
        timestamp_ns=deserialized_job.signature.timestamp_ns,
        signature=base64.b64encode(deserialized_job.signature.signature),
    )

    deserialized_payload = deserialized_job.json_for_signing()
    verifier = BittensorWalletVerifier()
    verifier.verify(deserialized_payload, deserialized_raw_signature)

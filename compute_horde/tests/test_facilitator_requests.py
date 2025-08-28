import base64

from compute_horde_core.executor_class import ExecutorClass
from compute_horde_core.signature import BittensorWalletSigner, Signature

from compute_horde.fv_protocol.facilitator_requests import V2JobRequest
from compute_horde.test_wallet import get_test_misc_wallet


class TestV2JobRequestNamespace:
    """Test namespace field in V2JobRequest protocol."""

    def test_v2_job_request_includes_namespace(self):
        job_request = V2JobRequest(
            uuid="test-uuid",
            executor_class=ExecutorClass.spin_up_4min__gpu_24gb,
            docker_image="test/image",
            job_namespace="SN123.1.0",
            args=[],
            env={},
            use_gpu=False,
            download_time_limit=300,
            execution_time_limit=600,
            upload_time_limit=300,
            streaming_start_time_limit=30,
        )

        signature_wallet = get_test_misc_wallet()
        signer = BittensorWalletSigner(signature_wallet)
        payload = job_request.json_for_signing()
        raw_signature = signer.sign(payload)

        job_request.signature = Signature(
            signature_type=raw_signature.signature_type,
            signatory=raw_signature.signatory,
            timestamp_ns=raw_signature.timestamp_ns,
            signature=base64.b64encode(raw_signature.signature),
        )

        # Test serialization
        json_data = job_request.model_dump()
        assert json_data["job_namespace"] == "SN123.1.0"

        # Test deserialization
        reconstructed = V2JobRequest.model_validate(json_data)
        assert reconstructed.job_namespace == "SN123.1.0"

    def test_v2_job_request_empty_namespace_default(self):
        job_request = V2JobRequest(
            uuid="test-uuid",
            executor_class=ExecutorClass.spin_up_4min__gpu_24gb,
            docker_image="test/image",
            args=[],
            env={},
            use_gpu=False,
            download_time_limit=300,
            execution_time_limit=600,
            upload_time_limit=300,
            streaming_start_time_limit=30,
        )

        signature_wallet = get_test_misc_wallet()
        signer = BittensorWalletSigner(signature_wallet)
        payload = job_request.json_for_signing()
        raw_signature = signer.sign(payload)

        job_request.signature = Signature(
            signature_type=raw_signature.signature_type,
            signatory=raw_signature.signatory,
            timestamp_ns=raw_signature.timestamp_ns,
            signature=base64.b64encode(raw_signature.signature),
        )
        assert job_request.job_namespace == ""

        # Test serialization
        json_data = job_request.model_dump()
        assert json_data["job_namespace"] == ""

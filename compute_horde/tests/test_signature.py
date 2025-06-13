import base64
import datetime

import freezegun
import pytest
from compute_horde_core.executor_class import ExecutorClass
from compute_horde_core.output_upload import SingleFilePutUpload
from compute_horde_core.signature import (
    BittensorWalletSigner,
    BittensorWalletVerifier,
    Signature,
    SignatureInvalidException,
    SignatureNotFound,
    SignedFields,
    hash_message_signature,
    signature_from_headers,
    signature_payload,
)
from compute_horde_core.volume import HuggingfaceVolume, MultiVolume, SingleFileVolume

from compute_horde.fv_protocol.facilitator_requests import (
    V2JobRequest,
    to_json_array,
)
from compute_horde.fv_protocol.validator_requests import V0AuthenticationRequest


@pytest.fixture
def sample_data():
    return {
        "b": 2,
        "array": [1, 2, 3],
        "dict": {"subdict:": {"null": None}},
    }


@pytest.fixture
def sample_signature():
    return Signature(
        signature_type="bittensor",
        signatory="5FUJCuGtQPonu8B9JKH4BwsKzEdtyBTpyvbBk2beNZ4iX8sk",  # hotkey address
        timestamp_ns=1718845323456788992,
        signature=base64.b64encode(
            base64.b85decode(
                "1SaAcLt*GG`2RG*@xmapXZ14m*Y`@b1MP(hAfEnwXkO5Os<30drw{`X`15JFP4GWR96T7p>rUmYA#=8Z"
            )
        ),
    )


def test_hash_message_signature(sample_data, sample_signature):
    assert hash_message_signature(sample_data, sample_signature) == base64.b85decode(
        "Dk7mtj^WM^#_n_!-C@$EHF*O@>;mdK%XS?515>IhxRvgxdT<nq$ImMmQ)jr-hpUSs#o+GQRX~t<q0-~1"
    )


@freezegun.freeze_time("2024-06-20T01:02:03.456789")
def test_bittensor_wallet_signer_sign(signature_wallet, sample_data):
    signer = BittensorWalletSigner(signature_wallet)
    signature = signer.sign(sample_data)

    assert signature == Signature(
        signature_type="bittensor",
        signatory=signature_wallet.hotkey.ss58_address,
        timestamp_ns=1718845323456788992,
        signature=base64.b64encode(signature.signature),
    )

    assert isinstance(signature.signature, bytes) and len(signature.signature) == 64


def test_bittensor_wallet_verifier_verify(sample_data, sample_signature):
    verifier = BittensorWalletVerifier()
    verifier.verify(sample_data, sample_signature)


def test_bittensor_wallet_verifier_verify_invalid_signature(sample_data, sample_signature):
    verifier = BittensorWalletVerifier()
    sample_signature.signature = b"invalid"
    with pytest.raises(SignatureInvalidException):
        verifier.verify(sample_data, sample_signature)


def test_bittensor_wallet_verifier_verify_signature_timeout(sample_data, sample_signature):
    verifier = BittensorWalletVerifier()

    with pytest.raises(SignatureInvalidException):
        verifier.verify(sample_data, sample_signature, newer_than=datetime.datetime.now())


@pytest.mark.parametrize(
    "data",
    [
        {"b": 2, "array": [1, 2, 3], "dict": {"subdict:": {"null": None}}},
        b"test",
    ],
)
def test_bittensor_wallet__sign_n_verify(signature_wallet, data):
    signer = BittensorWalletSigner(signature_wallet)
    verifier = BittensorWalletVerifier()

    signature = signer.sign(data)
    verifier.verify(data, signature)


def test_signature_from_headers__not_found():
    with pytest.raises(SignatureNotFound):
        signature_from_headers({})


def test_signature_payload():
    assert signature_payload(
        "get", "https://example.com/car", headers={"Date": "X"}, json={"a": 1}
    ) == {
        "action": "GET /car",
        "json": {"a": 1},
    }


def test_signed_fields__missing_fields():
    facilitator_request_json = {
        "executor_class": str(ExecutorClass.always_on__llm__a6000),
        "env": {},
        "use_gpu": False,
        "input_url": "",
        "uploads": [],
        "volumes": [],
        "download_time_limit": 1,
        "execution_time_limit": 1,
        "upload_time_limit": 1,
        "streaming_start_time_limit": 1,
    }
    facilitator_signed_fields = SignedFields.from_facilitator_sdk_json(facilitator_request_json)

    v2_job_request = V2JobRequest(
        uuid="uuid",
        executor_class=ExecutorClass.always_on__llm__a6000,
        docker_image="",
        args=[],
        env={},
        use_gpu=False,
        volume=None,
        output_upload=None,
        download_time_limit=1,
        execution_time_limit=1,
        upload_time_limit=1,
        streaming_start_time_limit=1,
    )
    assert v2_job_request.get_signed_fields() == facilitator_signed_fields


def test_signed_fields__volumes_uploads():
    volumes = [
        SingleFileVolume(
            url="smth.amazon.com",
            relative_path="np_data.npy",
        ),
        HuggingfaceVolume(
            repo_id="hug",
            revision="333",
            relative_path="./models/here",
        ),
    ]

    uploads = [
        SingleFilePutUpload(
            url="smth.amazon.com",
            relative_path="output.json",
        )
    ]

    facilitator_request_json = {
        "validator_hotkey": "5HBVrXXYYZZ",
        "random_field": "to_ignore",
        "executor_class": str(ExecutorClass.always_on__llm__a6000),
        "docker_image": "backenddevelopersltd/latest",
        "args": ["--device", "cuda", "--batch_size", "1", "--model_ids", "Deeptensorlab"],
        "env": {"f": "test"},
        "use_gpu": True,
        "input_url": "",
        "uploads": to_json_array(uploads),
        "volumes": to_json_array(volumes),
        "download_time_limit": 1,
        "execution_time_limit": 1,
        "upload_time_limit": 1,
        "streaming_start_time_limit": 1,
    }
    facilitator_signed_fields = SignedFields.from_facilitator_sdk_json(facilitator_request_json)

    v2_job_request = V2JobRequest(
        uuid="uuid",
        executor_class=ExecutorClass.always_on__llm__a6000,
        docker_image="backenddevelopersltd/latest",
        args=["--device", "cuda", "--batch_size", "1", "--model_ids", "Deeptensorlab"],
        env={"f": "test"},
        use_gpu=True,
        volume=MultiVolume(
            volumes=volumes,
        ),
        output_upload=uploads[0],
        download_time_limit=1,
        execution_time_limit=1,
        upload_time_limit=1,
        streaming_start_time_limit=1,
    )
    assert v2_job_request.get_signed_fields() == facilitator_signed_fields


def test_authentication_request(keypair):
    authentication_request = V0AuthenticationRequest.from_keypair(keypair)
    assert authentication_request.verify_signature()
    assert authentication_request.ss58_address == keypair.ss58_address

# compute-horde-facilitator-sdk
&nbsp;[![Continuous Integration](https://github.com/backend-developers-ltd/compute-horde-facilitator-sdk/workflows/Continuous%20Integration/badge.svg)](https://github.com/backend-developers-ltd/compute-horde-facilitator-sdk/actions?query=workflow%3A%22Continuous+Integration%22)&nbsp;[![License](https://img.shields.io/pypi/l/compute_horde_facilitator_sdk.svg?label=License)](https://pypi.python.org/pypi/compute_horde_facilitator_sdk)&nbsp;[![python versions](https://img.shields.io/pypi/pyversions/compute_horde_facilitator_sdk.svg?label=python%20versions)](https://pypi.python.org/pypi/compute_horde_facilitator_sdk)&nbsp;[![PyPI version](https://img.shields.io/pypi/v/compute_horde_facilitator_sdk.svg?label=PyPI%20version)](https://pypi.python.org/pypi/compute_horde_facilitator_sdk)

## Installation

```shell
pip install compute-horde-facilitator-sdk
```

## Usage

Register at https://facilitator.computehorde.io and generate an API Token.

> [!IMPORTANT]
> This package uses [ApiVer](#versioning), make sure to import `compute_horde_facilitator_sdk.v1`.

Example:

```python
import os
import time

from compute_horde.executor_class import DEFAULT_EXECUTOR_CLASS
from compute_horde_facilitator_sdk.v1 import BittensorWalletSigner, FacilitatorClient

signer = None
# Optionally, you can sign each request with your wallet for additional security & higher execution priority.
try:
    import bittensor

    signer = BittensorWalletSigner(bittensor.wallet())
except ImportError:
    print('bittensor package not installed, skipping wallet signing')


compute_horde = FacilitatorClient(
    token=os.environ["COMPUTE_HORDE_FACILITATOR_TOKEN"],
    signer=signer,
)

job = compute_horde.create_docker_job(
    executor_class=DEFAULT_EXECUTOR_CLASS,
    docker_image="backenddevelopersltd/gen_caption_v2",
    # The zip file will be extracted within the Docker container to the /volume directory
    input_url="https://raw.githubusercontent.com/backend-developers-ltd/ComputeHorde-examples/master/input_shapes.zip",
    use_gpu=True,
)

job = compute_horde.wait_for_job(job["uuid"])

print(
    f'Job finished with status: {job["status"]}. Stdout is: "{job["stdout"]}",'
    f' output_url is {job["output_download_url"]}'
)
# During job execution, any files generated in the /output directory will be incorporated into the final job result,
# which can be downloaded from the url printed above. Full STDOUT and STDERR will also be there.
```

For more information about Request Signing, see the [SDK signing documentation](docs/sdk_signatures.md).

## Versioning

This package uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
TL;DR you are safe to use [compatible release version specifier](https://packaging.python.org/en/latest/specifications/version-specifiers/#compatible-release) `~=MAJOR.MINOR` in your `pyproject.toml` or `requirements.txt`.

Additionally, this package uses [ApiVer](https://www.youtube.com/watch?v=FgcoAKchPjk) to further reduce the risk of breaking changes.
This means, the public API of this package is explicitly versioned, e.g. `compute_horde_facilitator_sdk.v1`, and will not change in a backwards-incompatible way even when `compute_horde_facilitator_sdk.v2` is released.

Internal packages, i.e. prefixed by `compute_horde_facilitator_sdk._` do not share these guarantees and may change in a backwards-incompatible way at any time even in patch releases.


## Development


Pre-requisites:
- [pdm](https://pdm.fming.dev/)
- [nox](https://nox.thea.codes/en/stable/)
- [docker](https://www.docker.com/) and [docker compose plugin](https://docs.docker.com/compose/)


Ideally, you should run `nox -t format lint` before every commit to ensure that the code is properly formatted and linted.
Before submitting a PR, make sure that tests pass as well, you can do so using:
```
nox -t check # equivalent to `nox -t format lint test`
```

If you wish to install dependencies into `.venv` so your IDE can pick them up, you can do so using:
```
pdm install --dev
```

### Release process

Run `nox -s make_release -- X.Y.Z` where `X.Y.Z` is the version you're releasing and follow the printed instructions.

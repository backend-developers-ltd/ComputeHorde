# ComputeHorde SDK – Scalable GPU Power for Subnet Owners & Validators 
&nbsp;[![Continuous Integration](https://github.com/backend-developers-ltd/compute-horde-sdk/workflows/Continuous%20Integration/badge.svg)](https://github.com/backend-developers-ltd/compute-horde-sdk/actions?query=workflow%3A%22Continuous+Integration%22)&nbsp;[![License](https://img.shields.io/pypi/l/compute_horde_sdk.svg?label=License)](https://pypi.python.org/pypi/compute_horde_sdk)&nbsp;[![python versions](https://img.shields.io/pypi/pyversions/compute_horde_sdk.svg?label=python%20versions)](https://pypi.python.org/pypi/compute_horde_sdk)&nbsp;[![PyPI version](https://img.shields.io/pypi/v/compute_horde_sdk.svg?label=PyPI%20version)](https://pypi.python.org/pypi/compute_horde_sdk)

The **ComputeHorde SDK** enables **Bittensor subnet owners to improve the quality** of their subnets by providing **validators with cost-effective, scalable compute resources**.
Instead of requiring validators to maintain their own physical GPUs, ComputeHorde offers on-demand decentralized access to trustworthy hardware, 
**reducing costs and increasing validation power**.

## Why Use ComputeHorde? 
:heavy_check_mark: **Lower Costs** – Validators don’t need to buy, manage, or maintain GPUs.

:heavy_check_mark: **Massive Scalability** – Instant access to as many trustworthy GPUs as needed. 

:heavy_check_mark: **Faster Validation** – Increased compute power leads to better validation. 

:heavy_check_mark: **Secure by Design** – Only computation tasks are offloaded; private keys & weight setting remain on validator machines. 

## How It Works for Subnet Owners
Subnet owners **prepare validation code** that can run **both on ComputeHorde and on physical GPUs**. 
This ensures validators can seamlessly opt in to use ComputeHorde’s computing power. 

By enabling ComputeHorde support in their subnet, **subnet owners** benefit by: 

:heavy_check_mark: **Attracting More Validators** – Lower costs and easier maintenance make validators more likely to participate. 

:heavy_check_mark: **Increasing Available Compute Power** – More GPU resources mean faster, higher-quality validation. 

:heavy_check_mark: **Improving Subnet Quality** – Stronger validation enhances the reliability and competitiveness of the subnet’s commodity. 

## How It Works for Validators
Each validator **chooses whether to use ComputeHorde**. To gain access to ComputeHorde’s stake-based compute power, a validator must: 

:heavy_check_mark: Be or become a ComputeHorde validator. 

:heavy_check_mark: Partner with an existing ComputeHorde validator. 

## We’re Here to Help 
We actively support **subnet owners and validators** in integrating the SDK—both technically and business-wise. 
If you need guidance, **reach out to us** on the [ComputeHorde Discord channel](https://discordapp.com/channels/799672011265015819/1201941624243109888), 
and we’ll ensure your validator is ComputeHorde-ready. 

For more details, see the [ComputeHorde README](https://github.com/backend-developers-ltd/ComputeHorde#readme). 

---

## Installation

To get started, install the ComputeHorde SDK with:  

```
pip install compute-horde-sdk
```

For detailed API documentation, visit the [ComputeHorde SDK Reference](TODO_API_REFERENCE_LINK).


> [!IMPORTANT]
> This package uses [ApiVer](#versioning), make sure to import `compute_horde_sdk.v1`.


## Running Jobs on ComputeHorde

The **ComputeHorde SDK** allows validators to request ComputeHorde execution effortlessly. 
Below are examples demonstrating **basic job execution**, **advanced job configuration**, and **job management**.

### **1. Requesting ComputeHorde Execution (Simplest Example)**
This minimal example shows how to submit a job to ComputeHorde with **basic parameters**:

```python
import asyncio
import bittensor
from compute_horde_sdk.v1 import ComputeHordeClient, ExecutorClass

wallet = bittensor.wallet(name="...", hotkey="...")

compute_horde_client = ComputeHordeClient(
    hotkey=wallet.hotkey,  # ss58 address
    compute_horde_validator_hotkey="...",  # usually the same as the hotkey above
)

async def main():
    # Create a job to run on the Compute Horde.
    job = await compute_horde_client.create_job(
        executor_class=ExecutorClass.always_on__llm__a6000,
        job_namespace="SN123.0",
        docker_image="my-username/my-image:latest",
    )

    await job.wait(timeout=10 * 60)

    print(job.status)  # should be "Completed".

asyncio.run(main())
```

### **2. Advanced Job Configuration**
This example demonstrates how to submit a job with **additional parameters**, including:
- arguments & environment variables
- input & output volume configuration
- artifact (results) storage

```python
import asyncio
import bittensor
from compute_horde_sdk.v1 import ComputeHordeClient, ExecutorClass, InlineInputVolume, HuggingfaceInputVolume, HTTPInputVolume, HTTPOutputVolume

wallet = bittensor.wallet(name="...", hotkey="...")

compute_horde_client = ComputeHordeClient(
    hotkey=wallet.hotkey,  # ss58 address
    compute_horde_validator_hotkey="...",  # usually the same as the hotkey above
)

async def main():
    # Create a job to run on the Compute Horde.
    job = await compute_horde_client.create_job(
        executor_class=ExecutorClass.always_on__llm__a6000,
        job_namespace="SN123.0",
        docker_image="my-username/my-image:latest",
        args=["main.py", "--block", "10000"],
        env={"HF_HUB_ENABLE_HF_TRANSFER": "1"},
        artifacts_dir="/artifacts",
        input_volumes={
            "/volume/models/model01": HuggingfaceInputVolume(
                repo_id="my-username/my-model",
            ),
            "/volume/data/version.txt": InlineInputVolume(
                contents="dmVyc2lvbj0y",
            ),
            "/volume/data/dataset.json": HTTPInputVolume(
                url="https://my-dataset-bucket.s3.amazonaws.com/sample-dataset/data.json",
            ),
        },
        output_volumes={
            "/output/image.png": HTTPOutputVolume(
                http_method="PUT",
                url="https://my-image-bucket.s3.amazonaws.com/images/image.png",
            ),
        },
    )

    await job.wait(timeout=10 * 60)

    print(job.status)  # should be "Completed".
    print(job.result)

asyncio.run(main())
```

### **3. Managing ComputeHorde Jobs**

#### **Retrieve a Job by UUID**
If you need to fetch a specific job, use:

```python
job = await client.get_job("7b522daa-e807-4094-8d96-99b9a863f960")
```

#### **Iterate Over All Jobs**  
To process all of your submitted jobs:  

```python
async for job in client.iter_jobs():
    process(job)
```

If `job.status` is `"Completed"`, the `job.result` should be available.

## Versioning

This package uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
TL;DR you are safe to use [compatible release version specifier](https://packaging.python.org/en/latest/specifications/version-specifiers/#compatible-release) `~=MAJOR.MINOR` in your `pyproject.toml` or `requirements.txt`.

Additionally, this package uses [ApiVer](https://www.youtube.com/watch?v=FgcoAKchPjk) to further reduce the risk of breaking changes.
This means, the public API of this package is explicitly versioned, e.g. `compute_horde_sdk.v1`, and will not change in a backwards-incompatible way even when `compute_horde_sdk.v2` is released.

Internal packages, i.e. prefixed by `compute_horde_sdk._` do not share these guarantees and may change in a backwards-incompatible way at any time even in patch releases.


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

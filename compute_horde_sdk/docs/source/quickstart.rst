.. currentmodule:: compute_horde_sdk.v1

########################
Quickstart
########################

Usage
=====

.. important::

    This package uses ApiVer, make sure to import ``compute_horde_sdk.v1``.

Running Jobs on ComputeHorde
----------------------------

.. code-block:: python

    import asyncio

    import bittensor_wallet

    from compute_horde_sdk.v1 import ComputeHordeClient, ComputeHordeJobSpec, ExecutorClass

    wallet = bittensor_wallet.Wallet(name="...", hotkey="...")

    compute_horde_client = ComputeHordeClient(
        hotkey=wallet.hotkey,
        compute_horde_validator_hotkey="...",  # usually the ss58_address of the hotkey above
    )


    async def main():
        # Define your job
        job_spec = ComputeHordeJobSpec(
            executor_class=ExecutorClass.always_on__llm__a6000,
            job_namespace="SN123.0",
            docker_image="my-username/my-image:latest",
        )

        # Run the job
        job = await compute_horde_client.run_until_complete(job_spec)

        print(job.status)  # should be "Completed".


    asyncio.run(main())


Advanced Job Configuration
--------------------------

This example demonstrates how to submit a job with additional parameters, including:

* arguments & environment variables
* input & output volume configuration
* artifact (results) storage

.. code-block:: python

    import asyncio

    import bittensor_wallet

    from compute_horde_sdk.v1 import (
        ComputeHordeClient,
        ComputeHordeJobSpec,
        ExecutorClass,
        HTTPInputVolume,
        HTTPOutputVolume,
        HuggingfaceInputVolume,
        InlineInputVolume,
    )

    wallet = bittensor_wallet.Wallet(name="...", hotkey="...")

    compute_horde_client = ComputeHordeClient(
        hotkey=wallet.hotkey,
        compute_horde_validator_hotkey="...",  # usually the ss58_address of the hotkey above
    )


    async def main():
        # Define your job
        job_spec = ComputeHordeJobSpec(
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

        # Run the job
        job = await compute_horde_client.run_until_complete(
            job_spec=job_spec,
            timeout=300,  # retry/wait for up to 300 seconds for the job to complete
            max_attempts=5,  # try at most 5 times, if the job fails
        )

        print(job.status)  # should be "Completed".
        print(job.result)


    asyncio.run(main())


Managing ComputeHorde Jobs
--------------------------

Retrieve a Job by UUID
^^^^^^^^^^^^^^^^^^^^^^

If you need to fetch a specific job, use:

.. code-block:: python

    job = await client.get_job("7b522daa-e807-4094-8d96-99b9a863f960")


Iterate Over All Jobs
^^^^^^^^^^^^^^^^^^^^^

To process all of your submitted jobs:

.. code-block:: python

    async for job in client.iter_jobs():
        process(job)

If ``job.status`` is :class:`ComputeHordeJobStatus.COMPLETED`, the ``job.result`` should be available.

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
    import bittensor
    from compute_horde_sdk.v1 import ComputeHordeClient, ExecutorClass

    wallet = bittensor.wallet(name="...", hotkey="...")

    compute_horde_client = ComputeHordeClient(
        hotkey=wallet.hotkey,
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


Advanced Job Configuration
--------------------------

This example demonstrates how to submit a job with additional parameters, including:

* arguments & environment variables
* input & output volume configuration
* artifact (results) storage

.. code-block:: python

    import asyncio
    import bittensor
    from compute_horde_sdk.v1 import ComputeHordeClient, ExecutorClass, InlineInputVolume, HuggingfaceInputVolume, HTTPInputVolume, HTTPOutputVolume

    wallet = bittensor.wallet(name="...", hotkey="...")

    compute_horde_client = ComputeHordeClient(
        hotkey=wallet.hotkey,
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


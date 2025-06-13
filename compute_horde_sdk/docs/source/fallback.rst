.. currentmodule:: compute_horde_sdk.v1

########################
Fallback
########################

Usage
=====

.. important::

    In order to use the fallback functionality, please install ComputeHorde SDK with ``fallback`` enabled (see :doc:`installation`).

.. important::

    This package uses ApiVer, make sure to import ``compute_horde_sdk.v1.fallback``.

Running Jobs on a Fallback Cloud
--------------------------------

If ComputeHorde is not operational for some reason,
you can provide a logic for running the job on a fallback cloud like `RunPod <https://www.runpod.io/>`_.

The fallback functionality uses `SkyPilot <https://skypilot.co/>`_ cluster management utility.

Running on Runpod
^^^^^^^^^^^^^^^^^

If you want to run your job on Runpod in case of any error:

.. code-block:: python

    import asyncio

    import bittensor_wallet

    from compute_horde_sdk.v1 import ComputeHordeClient, ComputeHordeJobSpec, ExecutorClass
    from compute_horde_sdk.v1.fallback import FallbackClient, FallbackJobSpec


    async def main():
        try:
            wallet = bittensor_wallet.Wallet(name="...", hotkey="...")

            compute_horde_client = ComputeHordeClient(
                hotkey=wallet.hotkey,
                compute_horde_validator_hotkey="...",  # usually the ss58_address of the hotkey above
            )

            # Define your job
            job_spec = ComputeHordeJobSpec(
                executor_class=ExecutorClass.always_on__llm__a6000,
                job_namespace="SN123.0",
                docker_image="my-username/my-image:latest",
            )

            # Run the job
            job = await compute_horde_client.run_until_complete(job_spec)
        except Exception:
            # Create the fallback client for Runpod
            fallback_client = FallbackClient("runpod", api_key=environ.get("RUNPOD_API_KEY"))

            # Define your fallback job base on the ComputeHorde spec
            fallback_spec = FallbackJobSpec.from_job_spec(spec, work_dir="/app", region="US")

            # Run the fallback job
            job = await fallback_client.run_until_complete(fallback_spec)

        print(job.status)  # should be "Completed".


    asyncio.run(main())

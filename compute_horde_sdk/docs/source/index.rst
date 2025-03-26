.. ComputeHorde SDK documentation master file, created by
   sphinx-quickstart on Wed Mar  5 15:40:08 2025.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

###############################
ComputeHorde SDK documentation
###############################

The **ComputeHorde SDK** is a Python library for Bittensor subnet validators to offload compute jobs to
ComputeHorde’s decentralized GPU infrastructure.

To use the SDK, you need the hotkey of a **ComputeHorde validator** that accepts traffic from you.
The common setup is to run jobs through your own ComputeHorde validator.

For context on why this approach matters and how ComputeHorde fits into the broader Bittensor ecosystem, see the
`ComputeHorde README <https://github.com/backend-developers-ltd/ComputeHorde#readme>`_ and the
`ComputeHorde SDK README <https://github.com/backend-developers-ltd/ComputeHorde/tree/master/compute_horde_sdk#readme>`_.

Cross-validation
----------------

The SDK handles job submission, tracking, and result retrieval. You are responsible for implementing
**cross-validation logic**, which involves re-running about **1–2% of jobs** on the validator’s **trusted miner** to detect inconsistencies. 

The SDK offers built-in support for submitting jobs to the trusted miner, 
but the logic around when and how to use it depends on the **nature of your jobs** and your subnet’s validation design.

.. toctree::
   :maxdepth: 2

   installation
   quickstart
   api_reference

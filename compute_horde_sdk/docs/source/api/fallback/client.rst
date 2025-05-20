Fallback Client
===============

The :class:`~compute_horde_sdk.v1.fallback.FallbackClient` class provides an interface for interacting with the fallback service,
which allows users to run jobs on fallback cloud like `RunPod <https://www.runpod.io/>`_.
It uses `SkyPilot <https://skypilot.co/>`_ for managing the cluster.
This class includes methods for creating, managing, and retrieving jobs.

.. autoclass:: compute_horde_sdk.v1.fallback.FallbackClient
    :exclude-members:


.. __init__ is excluded by default, this fixes that

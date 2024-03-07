import logging

import bittensor
from django.conf import settings

logger = logging.getLogger(__name__)


def announce_address_and_port():
    axon = bittensor.axon(
        wallet=settings.BITTENSOR_WALLET(),
        external_port=settings.BITTENSOR_MINER_PORT,
        external_ip=settings.BITTENSOR_MINER_ADDRESS if not settings.BITTENSOR_MINER_ADDRESS_IS_AUTO else None,
        port=settings.BITTENSOR_MINER_PORT,
        ip=settings.BITTENSOR_MINER_ADDRESS if not settings.BITTENSOR_MINER_ADDRESS_IS_AUTO else None,
    )
    subtensor = bittensor.subtensor(network=settings.BITTENSOR_NETWORK)
    axon.serve(netuid=settings.BITTENSOR_NETUID, subtensor=subtensor)
    logger.info(f"Announced port and address: {settings.BITTENSOR_MINER_ADDRESS}: {settings.BITTENSOR_MINER_PORT}")

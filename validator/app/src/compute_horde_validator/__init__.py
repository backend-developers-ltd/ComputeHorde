from bittensor.utils.btlogging import logging

# Stop bittensor from interfering with our loggers. Hopefully.
logging.enable_third_party_loggers()

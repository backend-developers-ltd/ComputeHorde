import logging
import os
import sys

import uvicorn


def main():
    """Main entry point for the Volume Manager Example."""
    # Configure basic logging
    log_level = os.environ.get("VOLUME_MANAGER_LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, log_level),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    logger = logging.getLogger(__name__)
    
    # Get configuration from environment
    port = int(os.environ.get("VOLUME_MANAGER_PORT", "8001"))
    
    try:
        uvicorn.run(
            "volume_manager.api:app",
            host="0.0.0.0",
            port=port,
            reload=False,
            log_level="info",
        )
    except KeyboardInterrupt:
        logger.info("Shutting down Volume Manager Example")
    except Exception as e:
        logger.error(f"Failed to start Volume Manager Example: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 
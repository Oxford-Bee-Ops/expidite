from __future__ import annotations

import signal
import threading

from expidite_rpi.core import configuration as root_cfg
from expidite_rpi.management.common import load_and_set_inventory
from expidite_rpi.management.iot_hub_client import IoTHubClient

logger = root_cfg.setup_logger("expidite")


def main() -> None:
    logger.info("Management service starting")

    stop_event = threading.Event()

    def _signal_handler(signum: int, _frame: object) -> None:
        logger.info(f"Management service received signal {signum}; shutting down")
        stop_event.set()

    signal.signal(signal.SIGTERM, _signal_handler)
    signal.signal(signal.SIGINT, _signal_handler)

    inventory = load_and_set_inventory()
    if not inventory:
        logger.warning("Failed to load fleet configuration; idling until stopped")
        stop_event.wait()
        return

    client: IoTHubClient | None = None
    keys = root_cfg.keys
    if keys is not None:
        try:
            client = IoTHubClient()
            client.start()
        except Exception:
            logger.exception("IoT Hub client failed to start; idling until stopped")
            client = None

    if client is None:
        logger.warning("IoT Hub not available; idling until stopped")

    logger.info("Management service running")
    stop_event.wait()

    logger.info("Management service stopping")
    if client is not None:
        client.stop()
    logger.info("Management service stopped")


if __name__ == "__main__":
    main()

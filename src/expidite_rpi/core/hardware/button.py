"""GPIO button input handler for Expidite to allow manually triggered recording."""

from __future__ import annotations

import time
from importlib import import_module
from types import ModuleType

from expidite_rpi.core import configuration as root_cfg

logger = root_cfg.setup_logger("expidite")


class ButtonInput:
    """Momentary button handler with software debounce."""

    def __init__(
        self,
        pin: int = 27,
        debounce_seconds: float = 0.08,
    ) -> None:
        self.pin = pin
        self.debounce_seconds = debounce_seconds
        self._gpio: ModuleType | None = None
        self._last_press_time = 0.0

        self._initialize_hardware()

    def _initialize_hardware(self) -> None:
        gpio_module = import_module("RPi.GPIO")
        if gpio_module:
            self._gpio = gpio_module
            self._gpio.setmode(gpio_module.BCM)
            self._gpio.setup(self.pin, gpio_module.IN, pull_up_down=gpio_module.PUD_UP)
            logger.info("Button initialized on GPIO %s", self.pin)
        else:
            logger.warning("RPi.GPIO module could not be imported")
            raise ImportError("RPi.GPIO library is required for ButtonInput but is not installed.") from None

    def is_pressed(self) -> bool:
        """Return True only on valid debounced press events."""
        if self._gpio is None:
            return False

        raw_pressed = self._gpio.input(self.pin) == self._gpio.LOW
        if not raw_pressed:
            return False

        now = time.monotonic()
        if now - self._last_press_time < self.debounce_seconds:
            return False

        self._last_press_time = now
        return True

    def wait_for_press(
        self,
        poll_interval_seconds: float = 0.1,
        break_interval: float = root_cfg.my_device.max_recording_timer,
    ) -> bool:
        """Block until a debounced button press is detected or the break interval is exceeded."""
        logger.info("Waiting for button press on GPIO %s", self.pin)
        start_time = time.monotonic()
        while True:
            if self.is_pressed():
                return True
            if time.monotonic() - start_time > break_interval:
                return False
            time.sleep(poll_interval_seconds)

    def cleanup(self) -> None:
        if self._gpio is not None:
            try:
                self._gpio.cleanup(self.pin)
            except Exception:
                logger.exception("Failed to cleanup button input")

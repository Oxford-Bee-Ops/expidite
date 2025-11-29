#!/usr/bin/env python3
"""
LED manager: reflect status file in GPIO using pinctrl.
The text is always a colour (green|red) followed by a status (on|off|blink:N), with an optional
blink rate.
For example:
    green:on
    red:blink:0.25
    green:off
Anything else will result in red:on.

The status is set as follows:
    - red:on indicates initial power up or error state
    - red:blink (0.25 fast) indicates rpi_installer running
    - red:blink (0.5 medium) indicates device_manager booting
    - green:on indicates expidite running normally
    - green:blink indicates expidite has lost internet connectivity but wifi is up
    - red:blink (2.0 slow) indicates Wifi failed
"""

import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from threading import Event, Thread
from types import FrameType
from typing import Optional

LED_STATUS_FILE = Path("/expidite") / "tmp" / "tmp_flags" / "LED_STATUS"
LOCK_FILE: Path = Path("/var/lock/led_control.lock")

@dataclass
class Pin:
    gpio_pin: str
    blink_stop: Event = field(default_factory=Event)
    blink_thread: Optional[Thread] = None

GREEN_PIN = Pin(gpio_pin=os.environ.get("LED_GPIO_PIN", "16"))
RED_PIN = Pin(gpio_pin=os.environ.get("LED_GPIO_PIN_RED", "26"))
PINS: dict[str, Pin] = {
    "green": GREEN_PIN,
    "red": RED_PIN,
}
MODES = ("on", "off", "blink")
POLL_INTERVAL = float(os.environ.get("LED_POLL_SEC", "1.0"))
DEFAULT_BLINK_RATE = 0.5
stop_event = Event()

def run_pinctrl(args: list[str]) -> None:
    cmd = ["pinctrl", *args]
    try:
        subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except subprocess.CalledProcessError:
        # If pinctrl fails, ignore but log to stderr
        print(f"pinctrl failed: {' '.join(cmd)}", file=sys.stderr)

def ensure_output(pin: Pin) -> None:
    run_pinctrl(["set", pin.gpio_pin, "op"])

def set_high(pin: Pin) -> None:
    ensure_output(pin)
    run_pinctrl(["set", pin.gpio_pin, "dh"])

def set_low(pin: Pin) -> None:
    ensure_output(pin)
    run_pinctrl(["set", pin.gpio_pin, "dl"])

def blink_loop(rate: float, pin: Pin) -> None:
    # rate is seconds for half-cycle (on or off)
    try:
        while not pin.blink_stop.wait(0):
            set_high(pin)
            if pin.blink_stop.wait(rate):
                break
            set_low(pin)
            if stop_event.wait(rate):
                break
    except Exception as e:
        print("blink thread error:", e, file=sys.stderr)

def start_blink(rate: float | None, pin: Pin) -> None:
    stop_blink(pin=pin)
    pin.blink_stop.clear()
    pin.blink_thread = Thread(target=blink_loop, args=(rate, pin), daemon=True)
    pin.blink_thread.start()

def stop_blink(pin: Pin) -> None:
    if pin.blink_thread and pin.blink_thread.is_alive():
        pin.blink_stop.set()
        pin.blink_thread.join(timeout=1.0)
    pin.blink_thread = None
    pin.blink_stop.clear()

def reset_status() -> None:
    stop_blink(GREEN_PIN)
    stop_blink(RED_PIN)
    set_low(GREEN_PIN)
    set_low(RED_PIN)

def parse_status(text: str) -> tuple[str, str, Optional[float]]:
    """The text is always a colour (green|red) followed by a status (on|off|blink:N), with an optional
    blink rate.
    For example:
        green:on
        red:blink:0.25
        green:off
    Anything else will result in red:on.
    """
    if not text:
        return ("red", "on", None)
    text = text.strip().lower()
    parts = text.split(":")
    status = "on"
    rate = DEFAULT_BLINK_RATE

    if len(parts) < 2:
        return ("red", status, rate)

    colour = parts[0]
    if colour not in PINS:
        colour = "red"

    status = parts[1]
    if status not in MODES:
        status = "on"

    if len(parts) == 3 and parts[1] == "blink":
        rate_str = parts[2]
        try:
            rate = float(rate_str) if rate_str else DEFAULT_BLINK_RATE
            if rate <= 0:
                rate = DEFAULT_BLINK_RATE
        except ValueError:
            rate = DEFAULT_BLINK_RATE

    return (colour, status, rate)

def read_status_file() -> str:
    try:
        if LED_STATUS_FILE.exists():
            with open(LED_STATUS_FILE) as f:
                return f.read()
        else:
            return "red:on"
    except Exception as e:
        print("Error reading status file:", e, file=sys.stderr)
        return "red:on"

def acquire_lock_or_exit() -> None:
    if LOCK_FILE.exists():
        print(f"Lock file {LOCK_FILE} exists, exiting.", file=sys.stderr)
        sys.exit(1)
    else:
        LOCK_FILE.parent.mkdir(parents=True, exist_ok=True)
        LOCK_FILE.touch(exist_ok=False)

def handle_signal(signum: int, frame: FrameType | None) -> None:
    stop_event.set()

def main() -> None:
    acquire_lock_or_exit()
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    last_text = None

    # Ensure pin configured to output initially
    ensure_output(GREEN_PIN)
    ensure_output(RED_PIN)

    try:
        while not stop_event.is_set():
            text = read_status_file()
            if text == last_text:
                time.sleep(POLL_INTERVAL)
                continue
            # Change detected
            reset_status()
            last_text = text
            colour, mode, rate = parse_status(text)
            print(f"led_control status: {colour}:{mode}:{rate if mode=='blink' else ''}")
            pin = PINS[colour]
            if mode == "on":
                set_high(pin)
            elif mode == "off" or mode is None:
                set_low(pin)
            elif mode == "blink":
                start_blink(rate, pin)
            time.sleep(POLL_INTERVAL)
    finally:
        stop_blink(GREEN_PIN)
        stop_blink(RED_PIN)
        # leave red LED on on exit
        set_high(RED_PIN)
        try:
            LOCK_FILE.unlink()
        except Exception:
            pass

if __name__ == "__main__":
    main()

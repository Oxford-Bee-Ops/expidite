#!/usr/bin/env python3
"""
LED manager: reflect status file in GPIO using pinctrl.
Status file example contents:
  on
  off
  blink:0.25
"""
import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from threading import Event, Thread
from typing import Optional

STATUS_FILE: Path = Path(os.environ.get("LED_STATUS_FILE", "/expidite/flags/led_status"))
LOCK_FILE: Path = Path("/var/lock/led_control.lock")

@dataclass
class Pin():
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

def run_pinctrl(args):
    cmd = ["pinctrl", *args]
    try:
        subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except subprocess.CalledProcessError:
        # If pinctrl fails, ignore but log to stderr
        print(f"pinctrl failed: {' '.join(cmd)}", file=sys.stderr)

def ensure_output(pin: Pin):
    run_pinctrl(["set", pin.gpio_pin, "op"])

def set_high(pin: Pin):
    ensure_output(pin)
    run_pinctrl(["set", pin.gpio_pin, "dh"])

def set_low(pin: Pin):
    ensure_output(pin)
    run_pinctrl(["set", pin.gpio_pin, "dl"])

def blink_loop(rate: float, pin: Pin):
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

def start_blink(rate, pin: Pin):
    stop_blink(pin=pin)
    pin.blink_stop.clear()
    pin.blink_thread = Thread(target=blink_loop, args=(rate, pin), daemon=True)
    pin.blink_thread.start()

def stop_blink(pin: Pin):
    if pin.blink_thread and pin.blink_thread.is_alive():
        pin.blink_stop.set()
        pin.blink_thread.join(timeout=1.0)
    pin.blink_thread = None
    pin.blink_stop.clear()

def reset_status():
    stop_blink(GREEN_PIN)
    stop_blink(RED_PIN)
    set_low(GREEN_PIN)
    set_low(RED_PIN)

def parse_status(text):
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
    parts = text.split(":", 1)
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

def read_status_file():
    try:
        with open(STATUS_FILE, "r") as f:
            return f.read()
    except FileNotFoundError:
        return None
    except Exception as e:
        print("Error reading status file:", e, file=sys.stderr)
        return None

def acquire_lock_or_exit():
    if LOCK_FILE.exists():
        print(f"Lock file {LOCK_FILE} exists, exiting.", file=sys.stderr)
        sys.exit(1)
    else:
        LOCK_FILE.parent.mkdir(parents=True, exist_ok=True)
        LOCK_FILE.touch(exist_ok=False)      

def handle_signal(signum, frame):
    stop_event.set()

def main():
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
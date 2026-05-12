##############################################################################################################
# Expidite Web Server
#
# A lightweight Flask web server that exposes a subset of the bcli functionality via a browser.
# Designed to run as a systemd service alongside the main expidite service on a Raspberry Pi.
#
# Endpoints:
#   /             - Dashboard with links to all actions
#   /status       - View device status
#   /config       - View device configuration
#   /update       - Trigger a software update (runs rpi_installer.sh)
#   /reboot       - Reboot the device (requires confirmation)
##############################################################################################################
from __future__ import annotations

import html
import subprocess
import threading
from pathlib import Path

from flask import Flask, request

from expidite_rpi.core import configuration as root_cfg
from expidite_rpi.core.edge_orchestrator import EdgeOrchestrator
from expidite_rpi.rpi_core import RpiCore

logger = root_cfg.setup_logger("expidite")

app = Flask(__name__)

_init_lock = threading.Lock()
_rpi_core: RpiCore | None = None


def _get_rpi_core() -> RpiCore:
    global _rpi_core
    if _rpi_core is None:
        with _init_lock:
            if _rpi_core is None:
                sc = RpiCore()
                inventory = root_cfg.load_configuration()
                if inventory:
                    sc.configure(inventory)
                _rpi_core = sc
    return _rpi_core


def _page(title: str, body: str) -> str:
    """Wrap *body* HTML in a minimal page shell."""
    return (
        "<!DOCTYPE html><html><head>"
        "<meta charset='utf-8'>"
        "<meta name='viewport' content='width=device-width, initial-scale=1'>"
        f"<title>{html.escape(title)} — Expidite</title>"
        "<style>"
        "body{font-family:system-ui,sans-serif;margin:2rem;background:#f5f5f5;color:#222}"
        "h1{margin-top:0}"
        "pre{background:#fff;padding:1rem;border:1px solid #ddd;border-radius:4px;"
        "overflow-x:auto;white-space:pre-wrap;word-wrap:break-word}"
        "a.btn{display:inline-block;padding:.6rem 1.2rem;margin:.3rem;border-radius:4px;"
        "text-decoration:none;color:#fff;background:#2563eb}"
        "a.btn:hover{background:#1d4ed8}"
        "a.btn.danger{background:#dc2626}"
        "a.btn.danger:hover{background:#b91c1c}"
        "a.btn.warn{background:#d97706}"
        "a.btn.warn:hover{background:#b45309}"
        "nav{margin-bottom:1.5rem}"
        ".confirm{background:#fff;padding:1.5rem;border:2px solid #dc2626;border-radius:8px;"
        "display:inline-block}"
        "</style>"
        "</head><body>"
        f"<nav><a class='btn' href='/'>Home</a></nav>"
        f"<h1>{html.escape(title)}</h1>"
        f"{body}"
        "</body></html>"
    )


# ── Dashboard ───────────────────────────────────────────────────────────────────


@app.route("/")
def index() -> str:
    device_name = root_cfg.my_device.name if root_cfg.my_device else "Unknown"
    device_id = root_cfg.my_device_id
    return _page(
        "Expidite Dashboard",
        f"<p>Device: <strong>{html.escape(device_name)}</strong> "
        f"(<code>{html.escape(device_id)}</code>)</p>"
        "<a class='btn' href='/status'>View Status</a>"
        "<a class='btn' href='/config'>View Config</a>"
        "<a class='btn warn' href='/update'>Update Software</a>"
        "<a class='btn danger' href='/reboot'>Reboot Device</a>",
    )


# ── View Status ─────────────────────────────────────────────────────────────────


@app.route("/status")
def status() -> str:
    try:
        sc = _get_rpi_core()
        text = sc.status(verbose=True)
    except Exception as exc:
        text = f"Error retrieving status: {exc}"
    return _page("Device Status", f"<pre>{html.escape(text)}</pre>")


# ── View Config ─────────────────────────────────────────────────────────────────


@app.route("/config")
def config() -> str:
    parts: list[str] = []
    try:
        orchestrator = EdgeOrchestrator.get_instance()
        orchestrator.load_config()
        parts.append("Sensors & Datastreams\n" + "=" * 40)
        for i, dptree in enumerate(orchestrator.dp_trees):
            sensor_cfg = dptree.sensor.config
            parts.append(
                f"{i}> {sensor_cfg.sensor_type} {sensor_cfg.sensor_index}  {sensor_cfg.sensor_model}"
            )
            if sensor_cfg.outputs:
                parts.extend(f"  {stream.type_id}: - {stream.description}" for stream in sensor_cfg.outputs)
    except Exception as exc:
        parts.append(f"Error loading sensor config: {exc}")

    if root_cfg.system_cfg:
        parts.append("\nSystem Configuration\n" + "=" * 40)
        expidite_version, user_code_version, python_version = root_cfg.get_version_info()
        parts.append(f"Expidite version: {expidite_version}")
        parts.append(f"User code version: {user_code_version}")
        parts.append(f"Python version: {python_version}")
        for field, value in root_cfg.system_cfg.model_dump().items():
            parts.append(f"{field}: {value}")

    try:
        sc = _get_rpi_core()
        parts.append("\nExpidite Configuration\n" + "=" * 40)
        parts.append(sc.display_configuration())
    except Exception as exc:
        parts.append(f"Error loading expidite config: {exc}")

    return _page("Device Configuration", f"<pre>{html.escape(chr(10).join(parts))}</pre>")


# ── Update Software ─────────────────────────────────────────────────────────────


@app.route("/update", methods=["GET", "POST"])
def update() -> str:
    if request.method == "GET":
        return _page(
            "Update Software",
            "<div class='confirm'>"
            "<p>This will run the installer script to pull the latest code and may trigger a reboot.</p>"
            "<form method='POST'>"
            "<button type='submit' class='btn warn' style='border:none;cursor:pointer;font-size:1rem'>"
            "Confirm Update</button>"
            "</form>"
            "</div>",
        )

    if not root_cfg.running_on_rpi:
        return _page("Update Software", "<pre>This command only works on a Raspberry Pi.</pre>")

    if not root_cfg.system_cfg.is_valid:
        return _page("Update Software", "<pre>system.cfg is not set. Please check your installation.</pre>")

    scripts_dir = Path.home() / root_cfg.system_cfg.venv_dir / "scripts"
    if not scripts_dir.exists():
        return _page(
            "Update Software",
            f"<pre>Error: scripts directory does not exist at {scripts_dir}.</pre>",
        )

    installer = "zero_installer.sh" if root_cfg.running_on_pi_zero else "rpi_installer.sh"
    cmd = f"sudo -u $USER {scripts_dir}/{installer}"

    def _run_update() -> None:
        try:
            subprocess.run(cmd, shell=True, check=False, capture_output=True)
        except Exception:
            logger.exception("Software update failed")

    threading.Thread(target=_run_update, daemon=True).start()
    return _page(
        "Update Software",
        "<pre>Software update started in the background.\n"
        "The device may reboot automatically if new code was installed.</pre>",
    )


# ── Reboot Device ───────────────────────────────────────────────────────────────


@app.route("/reboot", methods=["GET", "POST"])
def reboot() -> str:
    if request.method == "GET":
        return _page(
            "Reboot Device",
            "<div class='confirm'>"
            "<p>Are you sure you want to reboot this device?</p>"
            "<form method='POST'>"
            "<button type='submit' class='btn danger' style='border:none;cursor:pointer;font-size:1rem'>"
            "Confirm Reboot</button>"
            "</form>"
            " <a class='btn' href='/'>Cancel</a>"
            "</div>",
        )

    if not root_cfg.running_on_rpi:
        return _page("Reboot Device", "<pre>This command only works on a Raspberry Pi.</pre>")

    subprocess.Popen(["sudo", "reboot"])
    return _page("Reboot Device", "<pre>Rebooting now…</pre>")


# ── Entry point ─────────────────────────────────────────────────────────────────


def main() -> None:
    logger.info("Starting Expidite web server on port 5000")
    app.run(host="0.0.0.0", port=5000)  # noqa: S104


if __name__ == "__main__":
    main()

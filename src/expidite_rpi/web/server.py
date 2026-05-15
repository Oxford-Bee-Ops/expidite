##############################################################################################################
# Expidite Web Server
#
# A lightweight Flask web server that exposes an HTTP interface for device management.
# Designed to run as a systemd service alongside the main expidite service on a Raspberry Pi.
#
# Callers should provide user confirmation for disruptive actions.
#
# Endpoints:
#   /reboot              - Reboot the device.
#   /review_mode         - GET: check if review mode is enabled. POST: enter. DELETE: exit.
##############################################################################################################
from __future__ import annotations

import subprocess

from flask import Flask, jsonify
from flask.typing import ResponseReturnValue

from expidite_rpi.bcli import InteractiveMenu
from expidite_rpi.core import configuration as root_cfg

logger = root_cfg.setup_logger("expidite")

app = Flask(__name__)
menu = InteractiveMenu()


##############################################################################################################
# Reboot Device.


@app.route("/reboot", methods=["POST"])
def reboot() -> ResponseReturnValue:
    logger.info("Reboot device")
    if root_cfg.running_on_rpi:
        subprocess.Popen(["sudo", "reboot"])
    return "", 403


##############################################################################################################
# Review Mode.


@app.route("/review_mode", methods=["GET"])
def get_review_mode() -> ResponseReturnValue:
    enabled = menu.is_review_mode_enabled()
    return jsonify({"enabled": enabled})


@app.route("/review_mode", methods=["POST"])
def enter_review_mode() -> ResponseReturnValue:
    logger.info("Enter review mode")
    menu.enter_review_mode()
    return jsonify({"enabled": True})


@app.route("/review_mode", methods=["DELETE"])
def exit_review_mode() -> ResponseReturnValue:
    logger.info("Exit review mode")
    menu.exit_review_mode()
    return jsonify({"enabled": False})


##############################################################################################################
# Entry point.


def main() -> None:
    logger.info("Starting Expidite web server on port 5000")
    app.run(host="0.0.0.0", port=5000)  # noqa: S104


if __name__ == "__main__":
    main()

from __future__ import annotations

import base64
import getpass
import hashlib
import hmac
import subprocess
import threading
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

from azure.iot.device import (
    IoTHubDeviceClient,  # pyright: ignore[reportPrivateImportUsage]
    ProvisioningDeviceClient,  # pyright: ignore[reportPrivateImportUsage]
)

if TYPE_CHECKING:
    from azure.iot.device import MethodRequest  # pyright: ignore[reportPrivateImportUsage]
from expidite_rpi.core import configuration as root_cfg
from expidite_rpi.core.device_config_objects import FAILED_TO_LOAD
from expidite_rpi.management.bcli import InteractiveMenu
from expidite_rpi.management.ssh_tunnel import SshTunnelManager

logger = root_cfg.setup_logger("expidite")

DPS_HOST = "global.azure-devices-provisioning.net"


def _derive_device_key(group_key: str, registration_id: str) -> str:
    """Derive a per-device symmetric key from the DPS group enrollment key."""
    signing_key = base64.b64decode(group_key)
    signed_hmac = hmac.new(signing_key, registration_id.encode("utf-8"), hashlib.sha256)
    return base64.b64encode(signed_hmac.digest()).decode("utf-8")


class IoTHubClient:
    """Azure IoT Hub client for remote commands (direct methods).

    Uses DPS symmetric key group enrollment. Designed to run as a standalone service
    process, communicating with the main EdgeOrchestrator via filesystem flags.
    """

    def __init__(self) -> None:
        self._hub_client: IoTHubDeviceClient | None = None
        # Use the BCLI object for some commands. Ideally these would be extracted into a separate module.
        self.im = InteractiveMenu()
        self.ssh_tunnel_manager = SshTunnelManager()

    def start(self) -> None:
        """Provision via DPS and connect to IoT Hub."""
        keys = root_cfg.keys
        assert keys is not None
        if FAILED_TO_LOAD in {keys.dps_scope_id, keys.dps_primary_key}:
            logger.info("IoT Hub: DPS credentials not configured; cannot start")
            return

        device_id = root_cfg.my_device_id
        device_key = _derive_device_key(keys.dps_primary_key, device_id)

        logger.info(f"IoT Hub: Provisioning device {device_id} via DPS...")
        provisioning_client = ProvisioningDeviceClient.create_from_symmetric_key(
            provisioning_host=DPS_HOST,
            registration_id=device_id,
            id_scope=keys.dps_scope_id,
            symmetric_key=device_key,
        )
        result = provisioning_client.register()

        if result.status != "assigned":
            logger.warning(f"IoT Hub: DPS registration failed with status '{result.status}'")
            return

        if result.registration_state is None:
            logger.warning("IoT Hub: DPS registration returned no registration state")
            return

        assigned_hub = result.registration_state.assigned_hub
        logger.info(f"IoT Hub: Device assigned to {assigned_hub}")

        self._hub_client = IoTHubDeviceClient.create_from_symmetric_key(
            symmetric_key=device_key,
            hostname=assigned_hub,
            device_id=device_id,
        )
        self._hub_client.connect()
        self._hub_client.on_method_request_received = self._on_method_request

        logger.info("IoT Hub: Connected and handlers registered")

    def stop(self) -> None:
        """Disconnect from IoT Hub and clean up."""
        self.ssh_tunnel_manager.close_all()
        if self._hub_client is not None:
            try:
                self._hub_client.shutdown()
            except Exception:
                logger.warning("IoT Hub: Error during shutdown", exc_info=True)
            self._hub_client = None

        logger.info("IoT Hub: Disconnected")

    # ---- Direct method handlers ----

    def _on_method_request(self, method_request: MethodRequest) -> None:
        """Dispatch incoming direct method requests to the appropriate handler."""
        from azure.iot.device import MethodResponse  # pyright: ignore[reportPrivateImportUsage]

        method_name = method_request.name
        raw_payload = method_request.payload
        payload: dict[str, Any] = raw_payload if isinstance(raw_payload, dict) else {}

        handlers: dict[str, Any] = {
            "reboot": self._handle_reboot,
            "get_review_mode": self._handle_get_review_mode,
            "enter_review_mode": self._handle_enter_review_mode,
            "exit_review_mode": self._handle_exit_review_mode,
            "update_software": self._handle_update_software,
            "open_ssh_tunnel": self._handle_open_ssh_tunnel,
        }

        handler = handlers.get(method_name)
        if handler is None:
            logger.warning(f"IoT Hub: Unknown direct method '{method_name}'")
            response = MethodResponse.create_from_method_request(
                method_request, 404, {"error": f"Unknown method '{method_name}'"}
            )
        else:
            try:
                result = handler(payload)
                response = MethodResponse.create_from_method_request(method_request, 200, result)
            except Exception:
                logger.exception(f"IoT Hub: Direct method '{method_name}' failed")
                response = MethodResponse.create_from_method_request(
                    method_request, 500, {"error": f"Method '{method_name}' failed"}
                )

        if self._hub_client is not None:
            self._hub_client.send_method_response(response)

    def _handle_reboot(self, _payload: dict[str, Any]) -> dict[str, str]:
        logger.info("IoT Hub: Reboot requested")
        if not root_cfg.running_on_rpi:
            return {"error": "Not running on Raspberry Pi"}

        # Delay so that we can send a response first.
        def _delayed_reboot() -> None:
            time.sleep(2)
            logger.info("Reboot now")
            subprocess.run(["sudo", "reboot"], check=False)

        threading.Thread(target=_delayed_reboot, daemon=True).start()
        return {"status": "rebooting"}

    def _handle_get_review_mode(self, _payload: dict[str, Any]) -> dict[str, str]:
        logger.info("IoT Hub: Get review mode requested")
        return {"status": str(self.im.is_review_mode_enabled())}

    def _handle_enter_review_mode(self, _payload: dict[str, Any]) -> dict[str, str]:
        logger.info("IoT Hub: Enter review mode requested")
        self.im.enter_review_mode()
        return {"status": str(self.im.is_review_mode_enabled())}

    def _handle_exit_review_mode(self, _payload: dict[str, Any]) -> dict[str, str]:
        logger.info("IoT Hub: Exit review mode requested")
        self.im.exit_review_mode()
        return {"status": str(self.im.is_review_mode_enabled())}

    def _handle_update_software(self, _payload: dict[str, Any]) -> dict[str, str]:
        # Delay so that we can send a response first.
        def _delayed_update_software() -> None:
            time.sleep(2)
            logger.info("Run install script now")
            scripts_dir = Path.home() / root_cfg.system_cfg.venv_dir / "scripts"
            if root_cfg.running_on_pi_zero:
                script = scripts_dir / "zero_installer.sh"
            else:
                script = scripts_dir / "rpi_installer.sh"

            logger.info(f"IoT Hub: Update software via {script}, with user {getpass.getuser()}")
            # The installer self-logs to syslog (tee -> logger, tag EXPIDITE), so we discard the inherited
            # stdout/stderr here to avoid duplicating every line into this service's journal.
            subprocess.Popen(
                ["sudo", "-u", getpass.getuser(), str(script)],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )

        threading.Thread(target=_delayed_update_software, daemon=True).start()

        return {"status": "update_started"}

    def _handle_open_ssh_tunnel(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Open an on-demand outbound SSH tunnel to the portal.

        Validation is synchronous so the ack reports acceptance; the dial-out and byte pump run on a
        daemon thread. See docs/ssh-tunnel-protocol.md. The token is never logged.
        """
        session_id = payload.get("sessionId", "<unknown>")
        logger.info(f"IoT Hub: open_ssh_tunnel requested for session {session_id}")
        accepted, reason = self.ssh_tunnel_manager.open(payload)
        response: dict[str, Any] = {"accepted": accepted}
        if reason is not None:
            response["reason"] = reason
        return response

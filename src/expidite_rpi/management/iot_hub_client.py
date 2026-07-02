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
from expidite_rpi.core import reboot
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

    # Backoff bounds for the connect-retry loop (seconds).
    _CONNECT_RETRY_INITIAL_SECONDS = 10
    _CONNECT_RETRY_MAX_SECONDS = 300

    def __init__(self) -> None:
        self._hub_client: IoTHubDeviceClient | None = None
        # Use the BCLI object for some commands. Ideally these would be extracted into a separate module.
        self.im = InteractiveMenu()
        self.ssh_tunnel_manager = SshTunnelManager()
        self._stop_event = threading.Event()
        self._connect_thread: threading.Thread | None = None

    def start(self) -> None:
        """Begin connecting to IoT Hub in the background, retrying until successful.

        Provisioning and the initial connect can fail transiently: there may be no network yet just after a
        reboot, DPS may be briefly unavailable, or the system clock may not yet be NTP-synced (so the
        time-based SAS tokens are rejected). Rather than give up on the first failure - which would leave the
        device unreachable via IoT Hub until the next reboot - we retry with backoff on a daemon thread so the
        device recovers on its own. A wrong clock is therefore self-healing: it simply fails an attempt and is
        retried once NTP catches up. Once the initial connect succeeds, the azure-iot SDK's own
        connection_retry handles transient MQTT drops.
        """
        keys = root_cfg.keys
        assert keys is not None
        if FAILED_TO_LOAD in {keys.dps_scope_id, keys.dps_primary_key}:
            logger.info("IoT Hub: DPS credentials not configured; cannot start")
            return

        self._connect_thread = threading.Thread(
            target=self._connect_with_retry, name="iot-hub-connect", daemon=True
        )
        self._connect_thread.start()

    def _connect_with_retry(self) -> None:
        """Provision and connect, retrying with exponential backoff until it succeeds or stop is asked."""
        backoff = self._CONNECT_RETRY_INITIAL_SECONDS
        while not self._stop_event.is_set():
            try:
                self._provision_and_connect()
            except Exception:
                logger.warning(f"IoT Hub: connect attempt failed; retrying in {backoff}s", exc_info=True)
            else:
                # _provision_and_connect clears _hub_client again if stop() raced in during the attempt;
                # only announce success if a live client actually remains.
                if self._hub_client is not None:
                    logger.info("IoT Hub: Connected and handlers registered")
                return
            # Sleep for the backoff period, but wake immediately if asked to stop.
            if self._stop_event.wait(backoff):
                return
            backoff = min(backoff * 2, self._CONNECT_RETRY_MAX_SECONDS)

    def _provision_and_connect(self) -> None:
        """Provision via DPS and connect to IoT Hub. Raises on any failure so the caller can retry."""
        keys = root_cfg.keys
        assert keys is not None

        device_id = root_cfg.my_device_id
        device_key = _derive_device_key(keys.dps_primary_key, device_id)

        logger.info(f"IoT Hub: Provisioning device {device_id} via DPS...")
        # websockets=True tunnels the connection over port 443 instead of MQTT's default 8883. Raspberry Pi
        # Connect also uses 443, so this matches its reachability on networks that block 8883 outbound.
        provisioning_client = ProvisioningDeviceClient.create_from_symmetric_key(
            provisioning_host=DPS_HOST,
            registration_id=device_id,
            id_scope=keys.dps_scope_id,
            symmetric_key=device_key,
            websockets=True,
        )
        result = provisioning_client.register()

        if result.status != "assigned":
            msg = f"DPS registration failed with status '{result.status}'"
            raise RuntimeError(msg)
        if result.registration_state is None:
            msg = "DPS registration returned no registration state"
            raise RuntimeError(msg)

        assigned_hub = result.registration_state.assigned_hub
        logger.info(f"IoT Hub: Device assigned to {assigned_hub}")

        hub_client = IoTHubDeviceClient.create_from_symmetric_key(
            symmetric_key=device_key,
            hostname=assigned_hub,
            device_id=device_id,
            websockets=True,
        )
        try:
            hub_client.connect()
            hub_client.on_method_request_received = self._on_method_request
        except Exception:
            # Tear down the half-built client so a failed attempt doesn't leak the SDK's MQTT background
            # threads; a fresh client is constructed on the next retry.
            try:
                hub_client.shutdown()
            except Exception:
                logger.debug("IoT Hub: error tearing down client after failed connect", exc_info=True)
            raise

        self._hub_client = hub_client

        # If stop() ran while this attempt was in flight, it may have passed its own shutdown step before
        # this client existed. Tear it down now rather than leave a live connection behind.
        if self._stop_event.is_set():
            self._shutdown_hub_client()

    def _shutdown_hub_client(self) -> None:
        """Shut down and clear the live hub client.

        Tolerant of being called from stop() and the connect thread concurrently: whichever nulls the
        reference first wins and the other becomes a no-op.
        """
        hub_client = self._hub_client
        self._hub_client = None
        if hub_client is None:
            return
        try:
            hub_client.shutdown()
        except Exception:
            logger.warning("IoT Hub: Error during shutdown", exc_info=True)

    def stop(self) -> None:
        """Disconnect from IoT Hub and clean up."""
        self._stop_event.set()
        if self._connect_thread is not None:
            # Keep the join short: stop() must finish within the unit's TimeoutStopSec (10s) and this is
            # only one of several teardown steps. _stop_event can't interrupt a blocking register()/connect(),
            # so an offline device will hit this timeout; the connect thread is a daemon and dies with the
            # process, and a connect that lands after the join is cleaned up by the stop re-check in
            # _provision_and_connect.
            self._connect_thread.join(timeout=5)
            self._connect_thread = None

        self.ssh_tunnel_manager.close_all()
        self._shutdown_hub_client()

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

        # Managed reboot flushes queued data to the cloud / disk spool before rebooting; it runs on a
        # background thread, and the initial delay lets us send the method response first.
        reboot.request_managed_reboot("Reboot requested via IoT Hub", delay_seconds=2)
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

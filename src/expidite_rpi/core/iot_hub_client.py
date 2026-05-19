from __future__ import annotations

import base64
import hashlib
import hmac
import subprocess
import threading
import time
from dataclasses import fields as dc_fields
from enum import Enum
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from azure.iot.device import MethodRequest  # pyright: ignore[reportPrivateImportUsage]

from expidite_rpi.core import configuration as root_cfg
from expidite_rpi.core.api import LedsInstalled
from expidite_rpi.core.device_config_objects import (
    FAILED_TO_LOAD,
    DeviceCfg,
    WifiClient,
)
from expidite_rpi.utils import utils

logger = root_cfg.setup_logger("expidite")

DPS_HOST = "global.azure-devices-provisioning.net"

TWIN_UPDATABLE_FIELDS: set[str] = {
    "heart_beat_frequency",
    "env_sensor_frequency",
    "review_mode_frequency",
    "max_recording_timer",
    "log_level",
    "attempt_wifi_recovery",
    "leds_installed",
    "wifi_clients",
    "tags",
    "notes",
    "cc_for_upload",
    "cc_for_journals",
    "cc_for_system_records",
    "cc_for_fair",
    "cc_for_fair_latest",
    "cc_for_system_test",
    "cc_for_diagnostics_bundles",
}

RESTART_REQUIRED_FIELDS: set[str] = {
    "review_mode_frequency",
    "max_recording_timer",
    "leds_installed",
}

EXCLUDED_REPORTED_FIELDS: set[str] = {
    "dp_trees_create_method",
    "dp_trees_create_kwargs",
}


def _derive_device_key(group_key: str, registration_id: str) -> str:
    """Derive a per-device symmetric key from the DPS group enrollment key."""
    signing_key = base64.b64decode(group_key)
    signed_hmac = hmac.new(signing_key, registration_id.encode("utf-8"), hashlib.sha256)
    return base64.b64encode(signed_hmac.digest()).decode("utf-8")


def device_cfg_to_twin_dict(cfg: DeviceCfg) -> dict[str, Any]:
    """Serialize a DeviceCfg to a JSON-compatible dict for twin reported properties.

    Excludes non-serializable fields (Callables) and converts enums/dataclasses.
    """
    result: dict[str, Any] = {}
    for field in dc_fields(cfg):
        if field.name in EXCLUDED_REPORTED_FIELDS:
            continue
        value = getattr(cfg, field.name)
        if isinstance(value, list) and value and hasattr(value[0], "__dataclass_fields__"):
            result[field.name] = [{f.name: getattr(item, f.name) for f in dc_fields(item)} for item in value]
        elif isinstance(value, Enum):
            result[field.name] = value.value
        elif callable(value):
            continue
        else:
            result[field.name] = value
    return result


def wifi_clients_from_twin(raw: list[dict[str, Any]]) -> list[WifiClient]:
    """Deserialize a list of dicts from twin desired properties into WifiClient objects."""
    return [WifiClient(ssid=w["ssid"], priority=w["priority"], pw=w["pw"]) for w in raw]


class IoTHubClient:
    """Azure IoT Hub client for remote commands (direct methods) and fleet config (device twins).

    Uses DPS symmetric key group enrollment. If DPS credentials are not configured
    in keys.env, get_instance() returns None and the system runs without IoT Hub.
    """

    _instance: IoTHubClient | None = None
    _lock: threading.Lock = threading.Lock()

    def __init__(self) -> None:
        from azure.iot.device import IoTHubDeviceClient  # pyright: ignore[reportPrivateImportUsage]

        self._hub_client: IoTHubDeviceClient | None = None
        self._report_timer: utils.RepeatTimer | None = None

    @staticmethod
    def get_instance() -> IoTHubClient | None:
        """Return the singleton IoTHubClient, or None if DPS credentials are not configured."""
        if IoTHubClient._instance is not None:
            return IoTHubClient._instance

        with IoTHubClient._lock:
            if IoTHubClient._instance is not None:
                return IoTHubClient._instance  # type: ignore[unreachable]

            keys = root_cfg.keys
            if keys is None or FAILED_TO_LOAD in {keys.dps_scope_id, keys.dps_primary_key}:
                logger.info("IoT Hub: DPS credentials not configured; running without IoT Hub")
                return None

            IoTHubClient._instance = IoTHubClient()
            return IoTHubClient._instance

    def start(self) -> None:
        """Provision via DPS and connect to IoT Hub."""
        from azure.iot.device import (  # pyright: ignore[reportPrivateImportUsage]
            IoTHubDeviceClient,
            ProvisioningDeviceClient,
        )

        keys = root_cfg.keys
        assert keys is not None
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
        self._hub_client.on_twin_desired_properties_patch_received = self._on_twin_desired_properties_patch

        self._report_current_state()

        report_interval = float(root_cfg.my_device.heart_beat_frequency)
        self._report_timer = utils.RepeatTimer(report_interval, self._report_current_state)
        self._report_timer.start()

        logger.info("IoT Hub: Connected and handlers registered")

    def stop(self) -> None:
        """Disconnect from IoT Hub and clean up."""
        if self._report_timer is not None:
            self._report_timer.cancel()
            self._report_timer = None

        if self._hub_client is not None:
            try:
                self._hub_client.shutdown()
            except Exception:
                logger.warning("IoT Hub: Error during shutdown", exc_info=True)
            self._hub_client = None

        IoTHubClient._instance = None
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
            "stop": self._handle_stop,
            "start": self._handle_start,
            "trigger_sensing": self._handle_trigger_sensing,
            "enter_review_mode": self._handle_enter_review_mode,
            "exit_review_mode": self._handle_exit_review_mode,
            "update_software": self._handle_update_software,
            "get_status": self._handle_get_status,
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
        if not root_cfg.running_on_rpi:
            return {"error": "Reboot only supported on Raspberry Pi"}
        logger.info("IoT Hub: Reboot requested")

        def _delayed_reboot() -> None:
            time.sleep(2)
            subprocess.run(["sudo", "reboot"], check=False)

        threading.Thread(target=_delayed_reboot, daemon=True).start()
        return {"status": "rebooting"}

    def _handle_stop(self, payload: dict[str, Any]) -> dict[str, str]:
        pkill = payload.get("pkill", False)
        logger.info(f"IoT Hub: Stop requested (pkill={pkill})")
        root_cfg.STOP_EXPIDITE_FLAG.touch()
        if pkill and root_cfg.system_cfg and root_cfg.system_cfg.my_start_script != FAILED_TO_LOAD:
            subprocess.run(
                ["sudo", "pkill", "-f", f"python -m {root_cfg.system_cfg.my_start_script}"],
                check=False,
            )
        return {"status": "stopping"}

    def _handle_start(self, _payload: dict[str, Any]) -> dict[str, str]:
        if not root_cfg.running_on_rpi:
            return {"error": "Start only supported on Raspberry Pi"}
        logger.info("IoT Hub: Start requested")
        subprocess.run(["sudo", "systemctl", "reset-failed", "expidite.service"], check=False)
        subprocess.run(["sudo", "systemctl", "start", "expidite.service"], check=False)
        return {"status": "starting"}

    def _handle_trigger_sensing(self, payload: dict[str, Any]) -> dict[str, str]:
        duration = payload.get("duration", 60)
        logger.info(f"IoT Hub: Trigger sensing for {duration}s")
        with open(root_cfg.SENSOR_TRIGGER_FLAG, "w") as f:
            f.write(str(duration))
        return {"status": "sensing_triggered", "duration": str(duration)}

    def _handle_enter_review_mode(self, _payload: dict[str, Any]) -> dict[str, str]:
        logger.info("IoT Hub: Enter review mode requested")
        root_cfg.REVIEW_MODE_FLAG.write_text(str(int(time.time())))
        root_cfg.RESTART_EXPIDITE_FLAG.touch()
        return {"status": "review_mode_entered"}

    def _handle_exit_review_mode(self, _payload: dict[str, Any]) -> dict[str, str]:
        logger.info("IoT Hub: Exit review mode requested")
        root_cfg.REVIEW_MODE_FLAG.unlink(missing_ok=True)
        root_cfg.RESTART_EXPIDITE_FLAG.touch()
        return {"status": "review_mode_exited"}

    def _handle_update_software(self, _payload: dict[str, Any]) -> dict[str, str]:
        if not root_cfg.running_on_rpi:
            return {"error": "Update only supported on Raspberry Pi"}
        if root_cfg.system_cfg is None or not root_cfg.system_cfg.is_valid:
            return {"error": "System config not available"}
        from pathlib import Path

        scripts_dir = Path.home() / root_cfg.system_cfg.venv_dir / "scripts"
        if root_cfg.running_on_pi_zero:
            script = scripts_dir / "zero_installer.sh"
        else:
            script = scripts_dir / "rpi_installer.sh"
        if not script.exists():
            return {"error": f"Installer script not found at {script}"}
        import getpass

        logger.info(f"IoT Hub: Update software via {script}")
        subprocess.Popen(["sudo", "-u", getpass.getuser(), str(script)])
        return {"status": "update_started"}

    def _handle_get_status(self, _payload: dict[str, Any]) -> dict[str, str]:
        from expidite_rpi.core.edge_orchestrator import EdgeOrchestrator

        orch = EdgeOrchestrator.get_instance()
        return orch.status()

    # ---- Device twin handlers ----

    def _on_twin_desired_properties_patch(self, patch: dict[str, Any]) -> None:
        """Handle desired property updates from IoT Hub."""
        logger.info(f"IoT Hub: Received twin desired property patch with keys: {list(patch.keys())}")
        applied = self._apply_config_patch(patch)
        if applied:
            logger.info(f"IoT Hub: Applied twin properties: {applied}")
            self._report_current_state()

    def _apply_config_patch(self, patch: dict[str, Any]) -> dict[str, str]:
        """Apply a desired property patch to the running DeviceCfg.

        Returns a dict of field_name -> str(value) for the fields that were applied.
        """
        applied: dict[str, str] = {}
        needs_restart = False
        wifi_changed = False

        for key, raw_value in patch.items():
            if key.startswith("$"):
                continue
            if key not in TWIN_UPDATABLE_FIELDS:
                logger.warning(f"IoT Hub: Ignoring non-updatable twin property '{key}'")
                continue

            coerced_value: Any
            if key == "wifi_clients":
                coerced_value = wifi_clients_from_twin(raw_value)
                wifi_changed = True
            elif key == "leds_installed":
                coerced_value = LedsInstalled(raw_value)
            else:
                coerced_value = raw_value

            root_cfg.my_device.update_field(key, coerced_value)
            applied[key] = str(coerced_value)

            if key in RESTART_REQUIRED_FIELDS:
                needs_restart = True

            if key == "log_level":
                root_cfg.setup_logger("expidite", level=int(raw_value))

        if needs_restart:
            logger.info("IoT Hub: Twin patch requires restart; setting RESTART flag")
            root_cfg.RESTART_EXPIDITE_FLAG.touch()

        if wifi_changed and root_cfg.running_on_rpi:
            from expidite_rpi.core.edge_orchestrator import EdgeOrchestrator

            orch = EdgeOrchestrator.get_instance()
            if orch.device_manager is not None:
                orch.device_manager.inject_wifi_clients()

        return applied

    def _report_current_state(self) -> None:
        """Send current device config and runtime info as twin reported properties."""
        if self._hub_client is None:
            return

        reported = device_cfg_to_twin_dict(root_cfg.my_device)

        expidite_version, user_code_version, python_version = root_cfg.get_version_info()
        reported["expidite_version"] = expidite_version
        reported["user_code_version"] = user_code_version
        reported["python_version"] = python_version

        from expidite_rpi.core.edge_orchestrator import EdgeOrchestrator

        try:
            reported["orchestrator_status"] = EdgeOrchestrator.get_instance().get_status().value
        except Exception:
            reported["orchestrator_status"] = "unknown"

        reported["last_reported"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

        try:
            self._hub_client.patch_twin_reported_properties(reported)
        except Exception:
            logger.warning("IoT Hub: Failed to report twin properties", exc_info=True)

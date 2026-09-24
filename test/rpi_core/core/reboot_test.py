from pathlib import Path

import pytest

from expidite_rpi.core import configuration as root_cfg
from expidite_rpi.core import reboot
from expidite_rpi.scripts import boot_history
from expidite_rpi.utils import utils


@pytest.mark.unittest
def test_in_process_reboot_labels_request_and_survives_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    reasons: list[str] = []
    monkeypatch.setattr(root_cfg, "STOP_EXPIDITE_FLAG", tmp_path / "stop")
    monkeypatch.setattr(root_cfg, "EXPIDITE_IS_RUNNING_FLAG", tmp_path / "running")
    monkeypatch.setattr(boot_history, "record_reboot_request", reasons.append)

    def fail_reboot(*_args: object, **_kwargs: object) -> str:
        message = "reboot failed"
        raise RuntimeError(message)

    monkeypatch.setattr(utils, "run_cmd", fail_reboot)

    reboot._flush_and_reboot("low memory", delay_seconds=0, is_error=False)

    assert reasons == ["managed reboot: low memory"]


@pytest.mark.unittest
def test_external_reboot_preserves_caller_and_recovers_after_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reasons: list[str] = []
    commands: list[str] = []
    monkeypatch.setattr(boot_history, "record_reboot_request", reasons.append)

    def fake_run_cmd(command: str, **_kwargs: object) -> str:
        commands.append(command)
        if command == "sudo reboot":
            message = "reboot failed"
            raise RuntimeError(message)
        return ""

    monkeypatch.setattr(utils, "run_cmd", fake_run_cmd)

    reboot._stop_service_and_reboot("Reboot requested via BCLI", delay_seconds=0)

    assert reasons == ["service stop and reboot: Reboot requested via BCLI"]
    assert commands == [
        "sudo systemctl stop expidite.service",
        "sudo reboot",
        "sudo systemctl start expidite.service",
    ]

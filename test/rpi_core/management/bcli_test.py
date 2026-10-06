from unittest.mock import Mock, call

import click
import pytest

from expidite_rpi.core import configuration as root_cfg
from expidite_rpi.core.device_config_objects import SystemCfg
from expidite_rpi.management import bcli

pytestmark = pytest.mark.unittest


@pytest.fixture
def menu() -> bcli.InteractiveMenu:
    """Exercise menu behavior without initializing sensors or cloud connections."""
    return bcli.InteractiveMenu.__new__(bcli.InteractiveMenu)


@pytest.mark.parametrize(
    ("menu_name", "action_names"),
    [
        (
            "interactive_menu",
            (
                "view_rpi_core_config",
                "view_status",
                "validate_device",
                "sensing_menu",
                "maintenance_menu",
                "debug_menu",
            ),
        ),
        ("sensing_menu", ("trigger_sensing",)),
        (
            "debug_menu",
            (
                "run_network_test",
                "journalctl",
                "display_errors",
                "display_rpi_core_logs",
                "display_sensor_logs",
                "display_score_logs",
                "display_running_processes",
                "show_recordings",
                "show_crontab_entries",
                "nmap_ping_scan",
            ),
        ),
        (
            "maintenance_menu",
            (
                "update_software",
                "enable_rpi_connect",
                "review_mode",
                "start_rpi_core",
                "stop_rpi_core",
                "stop_rpi_core",
                "reboot_device",
                "update_storage_key",
            ),
        ),
    ],
)
def test_menu_choices_preserve_command_dispatch(
    menu: bcli.InteractiveMenu,
    menu_name: str,
    action_names: tuple[str, ...],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    actions = Mock()
    for name in set(action_names):
        monkeypatch.setattr(menu, name, getattr(actions, name))
    monkeypatch.setattr(bcli, "check_if_setup_required", Mock())
    prompt = Mock(side_effect=[*range(1, len(action_names) + 1), 0])
    monkeypatch.setattr(click, "prompt", prompt)

    getattr(menu, menu_name)()

    expected = []
    for index, name in enumerate(action_names, start=1):
        if name == "stop_rpi_core":
            expected.append(getattr(call, name)(pkill=index == 6))
        else:
            expected.append(getattr(call, name)())
    assert actions.mock_calls == expected
    assert prompt.call_count == len(action_names) + 1


def test_submenu_back_returns_to_main_menu(
    menu: bcli.InteractiveMenu, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    sensing = Mock()
    status = Mock()
    monkeypatch.setattr(menu, "trigger_sensing", sensing)
    monkeypatch.setattr(menu, "view_status", status)
    monkeypatch.setattr(bcli, "check_if_setup_required", Mock())
    monkeypatch.setattr(click, "prompt", Mock(side_effect=[4, 1, 0, 2, 0]))

    menu.interactive_menu()

    sensing.assert_called_once_with()
    status.assert_called_once_with()
    output = capsys.readouterr().out
    assert output.count("Main Menu:") == 3
    assert output.count("Sensing Menu:") == 2
    assert "0. Back to Main Menu" in output
    assert output.endswith("Exiting...\n")


def test_invalid_choices_retry_and_default_choice_returns(
    menu: bcli.InteractiveMenu, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    sensing = Mock()
    monkeypatch.setattr(menu, "trigger_sensing", sensing)
    prompt = Mock(side_effect=[ValueError("invalid input"), -1, 2, 0])
    monkeypatch.setattr(click, "prompt", prompt)

    menu.sensing_menu()

    sensing.assert_not_called()
    output = capsys.readouterr().out
    assert output.count("Invalid input. Please enter a number.") == 1
    assert output.count("Invalid choice. Please try again.") == 2
    assert prompt.call_args_list == [call("\nEnter your choice", type=int, default=0)] * 4


def test_menu_abort_propagates(menu: bcli.InteractiveMenu, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(click, "prompt", Mock(side_effect=click.Abort))
    with pytest.raises(click.Abort):
        menu.sensing_menu()


@pytest.mark.parametrize("platform", ["windows", "macos"])
def test_journal_platform_check_preserves_prompt_order(
    menu: bcli.InteractiveMenu,
    platform: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(root_cfg, "running_on_linux", False)
    monkeypatch.setattr(root_cfg, "running_on_windows", platform == "windows")
    monkeypatch.setattr(root_cfg, "running_on_macos", platform == "macos")
    getchar = Mock(return_value="n")
    monkeypatch.setattr(click, "getchar", getchar)
    process = Mock()
    process.stdout.readline.side_effect = KeyboardInterrupt
    popen = Mock(return_value=process)
    monkeypatch.setattr(bcli.subprocess, "Popen", popen)

    menu.journalctl()

    getchar.assert_called_once_with()
    output = capsys.readouterr().out
    assert output.startswith("Do you want to filter the logs? (y/n)\nn\nPress Ctrl+C to exit...\n\n")
    if platform == "windows":
        popen.assert_not_called()
        assert output.endswith("This command only works on Linux. Exiting...\n")
    else:
        popen.assert_called_once_with(
            ["journalctl", "-f"], stdout=bcli.subprocess.PIPE, stderr=bcli.subprocess.PIPE
        )
        assert "This command only works on Linux" not in output


@pytest.mark.parametrize(
    "command", ["display_errors", "display_rpi_core_logs", "display_sensor_logs", "display_score_logs"]
)
def test_log_commands_remain_available_on_macos(
    menu: bcli.InteractiveMenu, command: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(root_cfg, "running_on_linux", False)
    monkeypatch.setattr(root_cfg, "running_on_windows", False)
    monkeypatch.setattr(root_cfg, "running_on_macos", True)
    logs = Mock(return_value=[])
    monkeypatch.setattr(bcli.device_health, "get_logs", logs)
    monkeypatch.setattr(bcli.utils, "run_cmd", Mock(return_value=""))

    getattr(menu, command)()

    logs.assert_called_once()


@pytest.mark.parametrize(
    "command", ["enable_rpi_connect", "show_crontab_entries", "reboot_device", "run_network_test"]
)
def test_rpi_guard_blocks_on_other_linux_hosts(
    menu: bcli.InteractiveMenu,
    command: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(root_cfg, "running_on_linux", True)
    monkeypatch.setattr(root_cfg, "running_on_rpi", False)
    monkeypatch.setattr(root_cfg, "system_cfg", None)
    monkeypatch.setattr(click, "getchar", Mock(side_effect=AssertionError("Must not prompt")))

    getattr(menu, command)()

    preambles = {
        "enable_rpi_connect": "Enabling RPi Connect service...\n",
        "show_crontab_entries": f"{bcli.dash_line}\n# CRONTAB ENTRIES\n{bcli.dash_line}\n\n",
        "reboot_device": "",
        "run_network_test": f"{bcli.dash_line}\n# NETWORK INFO\n{bcli.dash_line}\n",
    }
    assert capsys.readouterr().out == f"{preambles[command]}This command only works on a Raspberry Pi\n"


@pytest.mark.parametrize("missing_config", [True, False])
@pytest.mark.parametrize(
    "command", ["display_running_processes", "update_software", "validate_device", "run_network_test"]
)
def test_config_guard_handles_missing_and_invalid_config(
    menu: bcli.InteractiveMenu,
    command: str,
    missing_config: bool,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(root_cfg, "running_on_linux", True)
    monkeypatch.setattr(root_cfg, "running_on_windows", False)
    monkeypatch.setattr(root_cfg, "running_on_rpi", True)
    monkeypatch.setattr(root_cfg, "system_cfg", None if missing_config else SystemCfg(is_valid=False))

    getattr(menu, command)()

    prefix = "ERROR: " if command == "validate_device" else ""
    preambles = {
        "display_running_processes": "",
        "update_software": "Running update to get latest code...\n",
        "validate_device": f"{bcli.dash_line}\n# VALIDATE DEVICE\n{bcli.dash_line}\n",
        "run_network_test": f"{bcli.dash_line}\n# NETWORK INFO\n{bcli.dash_line}\n",
    }
    assert capsys.readouterr().out == (
        f"{preambles[command]}{prefix}System.cfg is not set. Please check your installation.\n"
    )


def test_guard_reads_configuration_at_call_time(
    menu: bcli.InteractiveMenu, monkeypatch: pytest.MonkeyPatch
) -> None:
    processes = Mock(return_value={"example.start"})
    monkeypatch.setattr(bcli.utils, "check_running_processes", processes)
    monkeypatch.setattr(root_cfg, "system_cfg", None)
    menu.display_running_processes()
    processes.assert_not_called()

    monkeypatch.setattr(root_cfg, "system_cfg", SystemCfg(is_valid=True, my_start_script="example.start"))
    menu.display_running_processes()
    processes.assert_called_once_with(search_string="example.start")


def test_default_startup_remains_available_without_config(
    menu: bcli.InteractiveMenu, monkeypatch: pytest.MonkeyPatch
) -> None:
    core = Mock()
    monkeypatch.setattr(menu, "sc", core, raising=False)
    monkeypatch.setattr(root_cfg, "system_cfg", None)
    monkeypatch.setattr(root_cfg, "running_on_linux", False)
    monkeypatch.setattr(root_cfg, "running_on_rpi", False)
    monkeypatch.setattr(click, "getchar", Mock(return_value="y"))

    menu.start_rpi_core()

    core.start.assert_called_once_with()


def test_command_helpers_still_return_platform_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(root_cfg, "running_on_rpi", False)
    assert bcli.run_cmd("unused") == "This command only works on a Raspberry Pi"
    assert bcli.run_cmd_live_echo("unused") == "This command only works on a Raspberry Pi"

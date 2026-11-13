import subprocess
from collections import deque
from types import SimpleNamespace

import pytest

import expidite_rpi.core.configuration as root_cfg
from expidite_rpi.core import device_manager
from expidite_rpi.core.device_config_objects import WifiClient

logger = root_cfg.setup_logger("expidite")


class Test_device_manager:
    @pytest.mark.unittest
    def test_device_manager(self) -> None:
        logger.info("Run test_device_manager test")
        device_manager.DeviceManager()

    @pytest.mark.unittest
    def test_inject_wifi_clients_supports_open_networks(self, monkeypatch: pytest.MonkeyPatch) -> None:
        logger.info("Run test_inject_wifi_clients_supports_open_networks test")
        manager = device_manager.DeviceManager.__new__(device_manager.DeviceManager)
        manager.client_wlan = "wlan0"

        commands: list[str] = []

        def run_cmd_mock(cmd: str, ignore_errors: bool = False, grep_strs: list[str] | None = None) -> str:
            del ignore_errors, grep_strs
            commands.append(cmd)
            return ""

        monkeypatch.setattr(
            device_manager.root_cfg,
            "my_device",
            SimpleNamespace(wifi_clients=[WifiClient("open-net", 100, "")]),
        )
        monkeypatch.setattr(
            device_manager.utils,
            "run_cmd",
            run_cmd_mock,
        )

        manager.inject_wifi_clients()

        assert len(commands) == 2
        assert "wifi-sec.key-mgmt none" in commands[1]
        assert "wifi-sec.psk" not in commands[1]

    @pytest.mark.unittest
    def test_attempt_wifi_recovery_supports_open_networks(self, monkeypatch: pytest.MonkeyPatch) -> None:
        logger.info("Run test_attempt_wifi_recovery_supports_open_networks test")
        manager = device_manager.DeviceManager.__new__(device_manager.DeviceManager)
        manager.wifi_clients = [WifiClient("open-net", 100, "")]
        # 8 minutes into the run of failures is the slot for step 4, the explicit wifi connect.
        manager.fail_run_start = 1000.0
        manager.last_recovery_slot = None
        manager.reboot_requested = False

        commands: list[str] = []

        def run_cmd_mock(cmd: str, ignore_errors: bool = False, grep_strs: list[str] | None = None) -> str:
            del ignore_errors, grep_strs
            commands.append(cmd)
            return ""

        monkeypatch.setattr(device_manager, "monotonic", lambda: 1000.0 + 8 * 60 + 5)
        monkeypatch.setattr(device_manager, "sleep", lambda _: None)
        monkeypatch.setattr(
            device_manager.utils,
            "run_cmd",
            run_cmd_mock,
        )

        manager.attempt_wifi_recovery()

        assert len(commands) == 1
        assert commands[0] == "sudo nmcli dev wifi connect open-net"

    @pytest.mark.unittest
    @pytest.mark.parametrize(
        ("ping_stdout", "expected"),
        [
            (
                (
                    "PING 8.8.8.8 (8.8.8.8) 56(84) bytes of data.\n"
                    "64 bytes from 8.8.8.8: icmp_seq=1 ttl=117 time=14.2 ms\n\n"
                    "--- 8.8.8.8 ping statistics ---\n"
                    "1 packets transmitted, 1 received, 0% packet loss, time 0ms\n"
                    "rtt min/avg/max/mdev = 14.231/14.231/14.231/0.000 ms\n"
                ),
                14.2,
            ),
            ("64 bytes from 8.8.8.8: icmp_seq=1 ttl=117 time=812 ms\n", 812.0),
            (
                (
                    "PING 8.8.8.8 (8.8.8.8) 56(84) bytes of data.\n\n"
                    "--- 8.8.8.8 ping statistics ---\n"
                    "1 packets transmitted, 0 received, 100% packet loss, time 0ms\n"
                ),
                None,
            ),
            ("", None),
        ],
    )
    def test_parse_ping_rtt_ms(self, ping_stdout: str, expected: float | None) -> None:
        assert device_manager.parse_ping_rtt_ms(ping_stdout) == expected

    @pytest.mark.unittest
    def test_rtt_summary_reports_last_min_avg_and_max_of_recent_good_pings(self) -> None:
        manager = device_manager.DeviceManager.__new__(device_manager.DeviceManager)
        manager.recent_rtts_ms = deque(maxlen=3)
        assert manager.rtt_summary() == "no recent good ping RTTs"

        for rtt_ms in (900.0, 40.0, 50.0, 60.0):
            manager.recent_rtts_ms.append(rtt_ms)

        # The window keeps only the last three, so the 900ms ping has dropped out.
        assert manager.rtt_summary() == "good ping RTT ms (last/min/avg/max of 3): 60/40/50/60"

    @pytest.mark.unittest
    @pytest.mark.parametrize(
        ("ping_returncode", "syn_sent_found", "expected_rtts", "expected_action"),
        [
            (0, False, [14.2], "ok"),
            # ICMP works but TCP is stuck: the RTT is still recorded.
            (0, True, [14.2], "fail"),
            (1, False, [], "fail"),
        ],
    )
    def test_wifi_timer_callback_records_rtt_whenever_ping_replies(
        self,
        monkeypatch: pytest.MonkeyPatch,
        ping_returncode: int,
        syn_sent_found: bool,
        expected_rtts: list[float],
        expected_action: str,
    ) -> None:
        manager = device_manager.DeviceManager.__new__(device_manager.DeviceManager)
        manager.recent_rtts_ms = deque(maxlen=device_manager.PING_RTT_WINDOW)
        manager.log_counter = 1
        manager.wifi_log_frequency = 600

        def run_mock(args: list[str] | str, **kwargs: object) -> subprocess.CompletedProcess[str]:
            del kwargs
            if isinstance(args, list):
                stdout = (
                    "64 bytes from 8.8.8.8: icmp_seq=1 ttl=117 time=14.2 ms\n" if ping_returncode == 0 else ""
                )
                return subprocess.CompletedProcess(args, ping_returncode, stdout=stdout, stderr="")
            # grep returns 0 when it finds a match; ESTAB is never found here.
            found = syn_sent_found and "SYN_SENT" in args
            return subprocess.CompletedProcess(args, 0 if found else 1)

        actions: list[str] = []
        monkeypatch.setattr(device_manager.subprocess, "run", run_mock)
        monkeypatch.setattr(manager, "action_on_ping_ok", lambda: actions.append("ok"))
        monkeypatch.setattr(manager, "action_on_ping_fail", lambda: actions.append("fail"))

        manager.wifi_timer_callback()

        assert list(manager.recent_rtts_ms) == expected_rtts
        assert actions == [expected_action]


# The recovery step each command belongs to, used to identify which steps ran.
_RECOVERY_STEP_COMMANDS = {
    "sudo nmcli dev disconnect wlan0": 1,
    "sudo nmcli radio wifi off": 2,
    "sudo nmcli general reload": 3,
    "sudo nmcli dev wifi connect home-net password secret": 4,
    "sudo systemctl restart NetworkManager": 5,
}


class _RecoveryHarness:
    """A DeviceManager driven by fake ping results on a fake monotonic clock, recording recovery actions."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.now = 1000.0
        self.steps: list[tuple[float, int]] = []  # (seconds since the harness started, step)
        self.reboots: list[float] = []
        self._start = self.now

        manager = device_manager.DeviceManager.__new__(device_manager.DeviceManager)
        manager.ping_failure_count_all = 0
        manager.ping_success_count_all = 0
        manager.ping_failure_count_run = 0
        manager.ping_success_count_run = 0
        manager.recent_rtts_ms = deque(maxlen=device_manager.PING_RTT_WINDOW)
        manager.fail_run_start = None
        manager.last_recovery_slot = None
        manager.reboot_requested = False
        manager.last_ping_was_ok = True
        manager.current_state = device_manager.DeviceState.INTERNET_UP
        manager.client_wlan = "wlan0"
        manager.wifi_clients = [WifiClient("home-net", 100, "secret")]
        self.manager = manager

        def run_cmd_mock(cmd: str, ignore_errors: bool = False, grep_strs: list[str] | None = None) -> str:
            del ignore_errors, grep_strs
            if cmd in _RECOVERY_STEP_COMMANDS:
                self.steps.append((self.now - self._start, _RECOVERY_STEP_COMMANDS[cmd]))
            return ""

        def reboot_mock(reason: str, is_error: bool = False) -> None:
            del reason, is_error
            self.reboots.append(self.now - self._start)

        monkeypatch.setattr(device_manager.root_cfg, "my_device", SimpleNamespace(attempt_wifi_recovery=True))
        monkeypatch.setattr(device_manager.utils, "run_cmd", run_cmd_mock)
        monkeypatch.setattr(device_manager.reboot, "request_managed_reboot", reboot_mock)
        monkeypatch.setattr(device_manager, "monotonic", lambda: self.now)
        monkeypatch.setattr(device_manager, "sleep", lambda _: None)

    def fail_for(self, seconds: float, check_interval: float) -> None:
        """Fail one check now and then every check_interval seconds, for `seconds`."""
        end = self.now + seconds
        while self.now <= end:
            self.manager.action_on_ping_fail()
            self.now += check_interval

    def succeed(self) -> None:
        self.manager.action_on_ping_ok()
        self.now += 10.0


class Test_wifi_recovery_schedule:
    @pytest.mark.unittest
    @pytest.mark.parametrize("check_interval", [10.0, 12.5, 17.0])
    def test_steps_repeat_every_20_minutes_until_the_2_hour_reboot(
        self, monkeypatch: pytest.MonkeyPatch, check_interval: float
    ) -> None:
        harness = _RecoveryHarness(monkeypatch)

        harness.fail_for(3 * 3600, check_interval)

        # Six 20 minute cycles of the five steps, each step on the first check in its 2 minute slot.
        expected_steps = [(cycle, step) for cycle in range(6) for step in range(1, 6)]
        assert [step for _, step in harness.steps] == [step for _, step in expected_steps]
        for (elapsed, _), (cycle, step) in zip(harness.steps, expected_steps, strict=True):
            due = cycle * device_manager.RECOVERY_CYCLE_SECONDS + step * device_manager.RECOVERY_STEP_SECONDS
            assert due <= elapsed < due + check_interval
        # One reboot request, on the first check at or after 2 hours, however long failures continue.
        assert len(harness.reboots) == 1
        reboot_due = device_manager.REBOOT_AFTER_SECONDS
        assert reboot_due <= harness.reboots[0] < reboot_due + check_interval

    @pytest.mark.unittest
    def test_a_successful_check_restarts_the_schedule(self, monkeypatch: pytest.MonkeyPatch) -> None:
        harness = _RecoveryHarness(monkeypatch)

        # Step 1 runs 2 minutes into the first run; the run ends before step 2 is due.
        harness.fail_for(3 * 60, 10.0)
        assert [step for _, step in harness.steps] == [1]
        harness.succeed()

        # The new run times its schedule from its own first failure, so it starts again at step 1.
        restart = harness.now - 1000.0
        harness.fail_for(5 * 60, 10.0)
        assert [step for _, step in harness.steps] == [1, 1, 2]
        assert harness.steps[1][0] - restart >= device_manager.RECOVERY_STEP_SECONDS

    @pytest.mark.unittest
    def test_brief_failure_runs_never_trigger_recovery(self, monkeypatch: pytest.MonkeyPatch) -> None:
        harness = _RecoveryHarness(monkeypatch)

        # A link that keeps dropping out for under 2 minutes at a time, for longer than the reboot timeout.
        for _ in range(60):
            harness.fail_for(100, 10.0)
            harness.succeed()

        assert harness.steps == []
        assert harness.reboots == []

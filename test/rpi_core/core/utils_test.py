import datetime as dt
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from threading import Event, Thread
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from expidite_rpi.core import api
from expidite_rpi.core import configuration as root_cfg
from expidite_rpi.utils import utils

logger = root_cfg.setup_logger("expidite")


class Test_utils:
    @pytest.mark.unittest
    def test_display_cfg(self) -> None:
        logger.info("Run test_display_cfg test")
        assert root_cfg.my_device.display() != ""

    @pytest.mark.unittest
    def test_utc_to_str(self) -> None:
        logger.info("Run test_utc_to_str test")
        timestamp = api.utc_to_fname_str()
        assert len(timestamp) == len("20250101T010101000"), "Invalid timestamp length:" + timestamp

    @pytest.mark.unittest
    def test_utc_now(self) -> None:
        logger.info("Run test_utc_now test")
        # Get datetime_now and convert to a POSIX timestamp (float)
        dt_object = api.utc_now()
        dt_float = dt_object.timestamp()
        ts_of_float = str(datetime.fromtimestamp(dt_float, UTC))
        ts_of_dt = str(dt.datetime.now(UTC))
        print(
            "dt_float:" + str(dt_float) + " dt_float=>" + ts_of_float + "; dt.now()=>",
            ts_of_dt,
        )
        assert ts_of_float[:19] == ts_of_dt[:19], "ts_of_float and ts_of_dt are not the same"

        # Get our standard UTC timestamp and then convert it back to a POSIX timestamp (float)
        ts_of_utils_utc = api.str_to_iso(api.utc_to_fname_str(dt_float))
        print("ts_of_utils_utc:", ts_of_utils_utc, " ts_of_dt:", ts_of_dt)

    @pytest.mark.unittest
    def test_raise_warn(self) -> None:
        logger.info("Run test_raise_warn test")
        logmsg = root_cfg.RAISE_WARN() + "This is a test error message"
        logger.error(logmsg)
        assert logmsg.startswith(api.RAISE_WARN_TAG)

    @pytest.mark.unittest
    def test_run_video_cmd_uses_command_duration(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """run_video_cmd should derive its timeout from the command's own -t (milliseconds)."""
        captured: dict[str, float | None] = {}

        def fake_run_cmd(
            cmd: str,
            ignore_errors: bool = False,
            grep_strs: list[str] | None = None,
            timeout: float | None = None,
        ) -> str:
            captured["timeout"] = timeout
            return ""

        monkeypatch.setattr(utils, "run_cmd", fake_run_cmd)
        utils.run_video_cmd("rpicam-vid -o out.mp4 -t 180000", margin_s=60.0)
        # 180000ms = 180s, plus the 60s margin.
        assert captured["timeout"] == pytest.approx(240.0)

    @pytest.mark.unittest
    def test_run_video_cmd_falls_back_to_default_when_no_t(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Commands without a -t (e.g. rpicam-still review frames) get a bounded default timeout."""
        captured: dict[str, float | None] = {}

        def fake_run_cmd(
            cmd: str,
            ignore_errors: bool = False,
            grep_strs: list[str] | None = None,
            timeout: float | None = None,
        ) -> str:
            captured["timeout"] = timeout
            return ""

        monkeypatch.setattr(utils, "run_cmd", fake_run_cmd)
        utils.run_video_cmd("rpicam-still -o out.jpg", default_duration_s=10.0, margin_s=60.0)
        assert captured["timeout"] == pytest.approx(70.0)


# Long enough that a healthy check thread always finishes; short enough that a deadlock fails fast.
THREAD_JOIN_TIMEOUT_S = 10.0


class _MountHarness:
    """Stands in for the expidite mount and the module logger while exercising the load checks."""

    def __init__(self) -> None:
        self.percent = 0.0
        self.reads = 0
        # Widens the window in which racing threads can both pass the staleness gate.
        self.delay = 0.0
        self.logger = MagicMock()

    def disk_usage(self, _path: str) -> SimpleNamespace:
        self.reads += 1
        time.sleep(self.delay)
        return SimpleNamespace(percent=self.percent)

    def warnings(self) -> list[str]:
        return [str(call.args[0]) for call in self.logger.warning.call_args_list]

    def low_disk_warnings(self) -> list[str]:
        return [w for w in self.warnings() if "Failing to keep up" in w]


@pytest.fixture
def mount(monkeypatch: pytest.MonkeyPatch) -> _MountHarness:
    """Present as an RPi with a settable mount usage, and reset the module-level check caches.

    monkeypatch restores the cached globals afterwards, so the checks cannot leak state between tests.
    """
    harness = _MountHarness()
    monkeypatch.setattr(root_cfg, "running_on_rpi", True)
    monkeypatch.setattr(utils, "logger", harness.logger)
    monkeypatch.setattr(utils, "last_space_check", dt.datetime(1970, 1, 1, tzinfo=UTC))
    monkeypatch.setattr(utils, "last_space_check_value", 0.0)
    monkeypatch.setattr(utils, "last_space_check_outcome", False)
    monkeypatch.setattr(utils, "last_space_check_breakdown", "")
    monkeypatch.setattr(utils, "_summarise_mount_usage", lambda: "processing 9MB in 9 files")
    monkeypatch.setattr(utils, "last_temp_check", dt.datetime(1970, 1, 1, tzinfo=UTC))
    monkeypatch.setattr(utils, "last_temp_check_outcome", False)
    monkeypatch.setattr(utils.psutil, "disk_usage", harness.disk_usage)
    # sensors_temperatures is Linux-only, so it is absent on the dev machines running these tests.
    monkeypatch.setattr(utils.psutil, "sensors_temperatures", dict, raising=False)
    return harness


class Test_load_checks:
    @pytest.mark.unittest
    def test_failing_to_keep_up_warns_above_critical(self, mount: _MountHarness) -> None:
        """Critical mount usage stalls the sensors, and says so."""
        mount.percent = 80.0

        assert utils.failing_to_keep_up() is True
        assert len(mount.low_disk_warnings()) == 1
        assert "80.0%" in mount.low_disk_warnings()[0]

    @pytest.mark.unittest
    def test_failing_to_keep_up_quiet_below_critical(self, mount: _MountHarness) -> None:
        """Usage under the critical threshold is not a stall and must not raise the warning."""
        mount.percent = 50.0

        assert utils.failing_to_keep_up() is False
        assert mount.low_disk_warnings() == []

    @pytest.mark.unittest
    def test_low_disk_warning_survives_a_reduce_load_refresh(self, mount: _MountHarness) -> None:
        """Regression: the stall must be reported whichever check refreshes the shared reading first.

        Both checks share one cached reading and one clock. When reduce_load_advised() refreshed it first
        - the natural order in a sensor loop - it used to reset that clock silently, so failing_to_keep_up()
        returned a cached True and stalled every sensor thread without ever logging why.
        """
        mount.percent = 80.0

        assert utils.reduce_load_advised() is True
        assert utils.failing_to_keep_up() is True
        assert len(mount.low_disk_warnings()) == 1, "The stall was not reported"

    @pytest.mark.unittest
    def test_checks_share_one_disk_reading(self, mount: _MountHarness) -> None:
        """The point of the shared cache: one disk read serves both checks for the whole interval."""
        mount.percent = 10.0

        utils.reduce_load_advised()
        utils.failing_to_keep_up()
        utils.failing_to_keep_up()

        assert mount.reads == 1

    @pytest.mark.unittest
    def test_checks_are_inert_off_rpi(self, mount: _MountHarness, monkeypatch: pytest.MonkeyPatch) -> None:
        """Off an RPi there is no expidite mount to measure, so neither check reads the disk."""
        monkeypatch.setattr(root_cfg, "running_on_rpi", False)
        mount.percent = 99.0

        assert utils.failing_to_keep_up() is False
        assert utils.reduce_load_advised() is False
        assert mount.reads == 0

    @pytest.mark.unittest
    def test_concurrent_checks_refresh_the_reading_once(self, mount: _MountHarness) -> None:
        """Regression: sensor threads race here, so the cached state must be updated under a lock.

        Without one, every thread arriving while the reading is stale passes the staleness gate before any
        of them has published a timestamp, so they all read the mount and a caller can be handed the
        previous outcome marked fresh for the rest of the interval.
        """
        mount.percent = 80.0
        mount.delay = 0.05
        outcomes: list[bool | None] = [None] * 8

        def check(index: int) -> None:
            outcomes[index] = utils.failing_to_keep_up()

        threads = [Thread(target=check, args=(i,), daemon=True) for i in range(len(outcomes))]
        for thread in threads:
            thread.start()
        deadline = time.monotonic() + THREAD_JOIN_TIMEOUT_S
        for thread in threads:
            thread.join(timeout=max(0.0, deadline - time.monotonic()))

        assert not any(thread.is_alive() for thread in threads), "A check thread never completed"
        assert mount.reads == 1, f"The mount was read {mount.reads} times in one interval"
        assert outcomes == [True] * len(outcomes)

    @pytest.mark.unittest
    def test_mixed_concurrent_checks_do_not_deadlock(self, mount: _MountHarness) -> None:
        """Both checks take the shared lock, and reduce_load_advised() drives the other one.

        A lock-ordering mistake here deadlocks rather than failing an assertion, so the threads are joined
        with a timeout and checked for liveness.
        """
        mount.percent = 80.0
        mount.delay = 0.02
        checks: list[Callable[[], bool]] = [utils.failing_to_keep_up, utils.reduce_load_advised]
        completed: list[bool] = [False] * 8

        def check(index: int) -> None:
            checks[index % len(checks)]()
            completed[index] = True

        threads = [Thread(target=check, args=(i,), daemon=True) for i in range(len(completed))]
        for thread in threads:
            thread.start()
        deadline = time.monotonic() + THREAD_JOIN_TIMEOUT_S
        for thread in threads:
            thread.join(timeout=max(0.0, deadline - time.monotonic()))

        assert all(completed), "A check thread deadlocked on the shared lock"
        assert mount.reads == 1

    @pytest.mark.unittest
    def test_mount_walk_does_not_block_other_threads(
        self, mount: _MountHarness, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The walk stats every file on the mount, so it must not be held across _check_lock.

        It is slowest exactly when the mount is fullest, which is when the most sensor threads are calling
        continue_recording(). Held under the lock it would serialise all of them behind one walk.
        """
        walking = Event()
        release = Event()

        def slow_walk() -> str:
            walking.set()
            release.wait(THREAD_JOIN_TIMEOUT_S)
            return "processing 9MB in 9 files"

        monkeypatch.setattr(utils, "_summarise_mount_usage", slow_walk)
        mount.percent = 80.0

        walker = Thread(target=utils.failing_to_keep_up, daemon=True)
        walker.start()
        assert walking.wait(THREAD_JOIN_TIMEOUT_S), "The walk never started"

        # The walk is in flight; a second thread must be handed the cached reading, not made to wait.
        start = time.monotonic()
        assert utils.failing_to_keep_up() is True
        elapsed = time.monotonic() - start

        release.set()
        walker.join(THREAD_JOIN_TIMEOUT_S)
        assert not walker.is_alive(), "The walking thread never completed"
        assert elapsed < 1.0, f"failing_to_keep_up() blocked for {elapsed:.1f}s behind the mount walk"

    @pytest.mark.unittest
    def test_mount_breakdown_names_every_entry(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Every top-level directory is named, biggest first, including the empty ones.

        A stage sitting at 0 files is what rules that stage out as the cause of the backlog.
        """
        for name, files, size in [("processing", 2, 400000), ("upload", 0, 0), ("logs", 1, 3000)]:
            (tmp_path / name / "nested").mkdir(parents=True)
            for i in range(files):
                (tmp_path / name / "nested" / f"f{i}.dat").write_bytes(b"x" * size)
        monkeypatch.setattr(root_cfg, "ROOT_WORKING_DIR", tmp_path)

        breakdown = utils._summarise_mount_usage()

        assert breakdown == "processing 0.8MB in 2 files, logs 0.0MB in 1 file, upload 0.0MB in 0 files"

    @pytest.mark.unittest
    def test_mount_breakdown_survives_an_unreadable_mount(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The breakdown is diagnostic text, so a failure to read the mount must not lose the warning."""
        monkeypatch.setattr(root_cfg, "ROOT_WORKING_DIR", Path("no-such-mount"))

        assert "breakdown unavailable" in utils._summarise_mount_usage()

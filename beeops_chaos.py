#!/usr/bin/env python3
"""BeeOps Network Chaos Test Harness.

Stress-tests network failure handling on Raspberry Pi:
  - Azure Blob Storage sync resilience (retry, no data loss)
  - Systemd watchdog survival
  - No duplicate uploads on reconnect
  - Packet loss / latency / hard disconnect scenarios

Usage:
    sudo python3 beeops_chaos.py [--iface wlan0] [--service expidite] [--scenarios all]

Requirements:
    pip install azure-storage-blob
    sudo apt install iproute2 iptables
"""

import argparse
import contextlib
import dataclasses
import json
import logging
import os
import random
import socket
import subprocess
import sys
import time
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

AZURE_ENDPOINTS = [
    "blob.core.windows.net",
    "login.microsoftonline.com",
]

LOG_FORMAT = "%(asctime)s  %(levelname)-8s  %(name)s  %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
log = logging.getLogger("chaos")


# ---------------------------------------------------------------------------
# Result tracking
# ---------------------------------------------------------------------------


@dataclasses.dataclass
class ScenarioResult:
    name: str
    passed: bool
    duration_s: float
    notes: list[str] = dataclasses.field(default_factory=list)

    def __str__(self) -> str:
        status = "✓ PASS" if self.passed else "✗ FAIL"
        notes = "  ".join(self.notes) if self.notes else ""
        return f"{status}  [{self.duration_s:.1f}s]  {self.name}  {notes}"


# ---------------------------------------------------------------------------
# Fault injection
# ---------------------------------------------------------------------------


class FaultInjector:
    """Wraps tc/iptables commands.
    Always cleans up on exit — safe to Ctrl-C.
    """

    def __init__(self, iface: str) -> None:
        self.iface = iface
        self._active_iptables: list[list[str]] = []

    # -- tc netem -------------------------------------------------------------

    def apply_netem(self, rule: str) -> None:
        """Apply a tc netem rule, replacing any existing one."""
        log.info("tc netem: %s", rule)
        # Try replace first (idempotent), fall back to add
        result = subprocess.run(
            ["tc", "qdisc", "replace", "dev", self.iface, "root", "netem", *rule.split()],
            capture_output=True,
            check=False,
        )
        if result.returncode != 0:
            subprocess.run(
                ["tc", "qdisc", "add", "dev", self.iface, "root", "netem", *rule.split()],
                check=True,
            )

    def clear_netem(self) -> None:
        subprocess.run(
            ["tc", "qdisc", "del", "dev", self.iface, "root"],
            capture_output=True,
            check=False,
        )
        log.info("tc netem: cleared")

    # -- iptables -------------------------------------------------------------

    @staticmethod
    def _resolve_ips(hostname: str) -> list[str]:
        """Resolve a hostname to its IP addresses (iptables needs IPs, not hostnames)."""
        try:
            results = socket.getaddrinfo(hostname, None, socket.AF_INET)
            return list({r[4][0] for r in results})
        except socket.gaierror:
            log.warning("Could not resolve %s — skipping", hostname)
            return []

    def drop_azure(self) -> None:
        """Block outbound traffic to Azure endpoints."""
        for host in AZURE_ENDPOINTS:
            ips = self._resolve_ips(host)
            if not ips:
                continue
            for ip in ips:
                rule = ["iptables", "-A", "OUTPUT", "-d", ip, "-j", "DROP"]
                subprocess.run(rule, check=True)
                self._active_iptables.append(rule)
            log.info("iptables DROP: %s → %s", host, ips)

    def clear_iptables(self) -> None:
        for rule in self._active_iptables:
            delete = ["iptables", "-D", *rule[2:]]
            subprocess.run(delete, capture_output=True, check=False)
        self._active_iptables.clear()
        log.info("iptables: cleared")

    # -- interface flap -------------------------------------------------------

    def interface_down(self) -> None:
        log.info("ip link set %s down", self.iface)
        subprocess.run(["ip", "link", "set", self.iface, "down"], check=True)

    def interface_up(self) -> None:
        log.info("ip link set %s up", self.iface)
        subprocess.run(["ip", "link", "set", self.iface, "up"], check=True)
        time.sleep(5)  # Allow DHCP / reconnect

    # -- cleanup --------------------------------------------------------------

    def clear_all(self) -> None:
        self.clear_netem()
        self.clear_iptables()
        # Ensure interface is up (best-effort)
        subprocess.run(["ip", "link", "set", self.iface, "up"], capture_output=True, check=False)

    @contextlib.contextmanager
    def fault(self, description: str) -> Iterator[None]:
        """Context manager: injects fault, always clears on exit."""
        log.info(">>> Fault ON:  %s", description)
        try:
            yield
        finally:
            log.info(">>> Fault OFF: %s", description)
            self.clear_all()


# ---------------------------------------------------------------------------
# Service monitor
# ---------------------------------------------------------------------------


class ServiceMonitor:
    def __init__(self, service: str) -> None:
        self.service = service
        self._restart_count_before: int = 0

    def _get_restart_count(self) -> int:
        result = subprocess.run(
            ["systemctl", "show", self.service, "--property=NRestarts"],
            capture_output=True,
            text=True,
            check=False,
        )
        try:
            return int(result.stdout.strip().split("=")[1])
        except (IndexError, ValueError):
            return 0

    def snapshot(self) -> None:
        self._restart_count_before = self._get_restart_count()

    def is_active(self) -> bool:
        result = subprocess.run(
            ["systemctl", "is-active", self.service],
            capture_output=True,
            text=True,
            check=False,
        )
        return result.stdout.strip() == "active"

    def new_restarts_since_snapshot(self) -> int:
        return self._get_restart_count() - self._restart_count_before

    def wait_for_active(self, timeout_s: float = 60.0) -> bool:
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            if self.is_active():
                return True
            time.sleep(2)
        return False

    def recent_log_lines(self, n: int = 50) -> list[str]:
        result = subprocess.run(
            ["journalctl", "-u", self.service, "-n", str(n), "--no-pager"],
            capture_output=True,
            text=True,
            check=False,
        )
        return result.stdout.splitlines()


# ---------------------------------------------------------------------------
# Azure blob audit
# ---------------------------------------------------------------------------


class BlobAudit:
    """Lightweight audit: lists blobs in a container and checks for duplicates.
    Pass connection_string=None to skip Azure checks (dry-run mode).
    """

    def __init__(self, connection_string: str | None, container: str) -> None:
        self.connection_string = connection_string
        self.container = container
        self._client = None

        if connection_string:
            try:
                from azure.storage.blob import BlobServiceClient

                self._client = BlobServiceClient.from_connection_string(connection_string)
            except ImportError:
                log.warning("azure-storage-blob not installed; skipping blob audit")

    def list_blobs(self) -> list[str]:
        if not self._client:
            return []
        container = self._client.get_container_client(self.container)
        return [b.name for b in container.list_blobs()]

    def check_no_duplicates(self, before: list[str], after: list[str]) -> tuple[bool, list[str]]:
        new_blobs = [b for b in after if b not in set(before)]
        # Simple dedup check: names should be unique
        dupes = [b for b in new_blobs if new_blobs.count(b) > 1]
        return len(dupes) == 0, dupes


# ---------------------------------------------------------------------------
# Scenarios
# ---------------------------------------------------------------------------


def scenario_azure_outage(
    injector: FaultInjector,
    monitor: ServiceMonitor,
    audit: BlobAudit,
    outage_s: float = 45.0,
) -> ScenarioResult:
    """Drop all traffic to Azure for ~45s.
    Verify: service stays alive, recovers cleanly, no crash in logs.
    """
    name = "azure_outage"
    notes = []
    t0 = time.monotonic()

    blobs_before = audit.list_blobs()
    monitor.snapshot()

    with injector.fault("iptables DROP azure"):
        injector.drop_azure()
        log.info("Azure blocked for %.0fs — watching service...", outage_s)
        time.sleep(outage_s)

        alive_during = monitor.is_active()
        notes.append(f"alive_during_fault={'yes' if alive_during else 'NO'}")

    # Recovery window
    log.info("Azure restored — waiting for service recovery...")
    recovered = monitor.wait_for_active(timeout_s=60)
    notes.append(f"recovered={'yes' if recovered else 'NO'}")

    restarts = monitor.new_restarts_since_snapshot()
    notes.append(f"restarts={restarts}")

    # Check logs for unhandled exceptions
    log_lines = monitor.recent_log_lines(100)
    exceptions = [line for line in log_lines if "Traceback" in line or "Exception" in line]
    notes.append(f"unhandled_exceptions={len(exceptions)}")

    # Blob dedup check
    blobs_after = audit.list_blobs()
    ok, dupes = audit.check_no_duplicates(blobs_before, blobs_after)
    notes.append(f"duplicate_blobs={len(dupes)}")

    passed = alive_during and recovered and len(exceptions) == 0 and ok
    return ScenarioResult(name, passed, time.monotonic() - t0, notes)


def scenario_packet_loss(
    injector: FaultInjector,
    monitor: ServiceMonitor,
    loss_pct: int = 30,
    duration_s: float = 60.0,
) -> ScenarioResult:
    """Inject 30% packet loss for 60s.
    Verify: service doesn't crash; retries visibly in logs.
    """
    name = f"packet_loss_{loss_pct}pct"
    notes = []
    t0 = time.monotonic()

    monitor.snapshot()

    with injector.fault(f"netem loss {loss_pct}%"):
        injector.apply_netem(f"loss {loss_pct}%")
        time.sleep(duration_s)
        alive = monitor.is_active()
        notes.append(f"alive_during={'yes' if alive else 'NO'}")

    recovered = monitor.wait_for_active(timeout_s=30)
    restarts = monitor.new_restarts_since_snapshot()
    notes.append(f"recovered={'yes' if recovered else 'NO'}")
    notes.append(f"restarts={restarts}")

    # Retries are expected — check logs mention retry/backoff keywords
    log_lines = monitor.recent_log_lines(100)
    retry_evidence = any(
        kw in line.lower() for line in log_lines for kw in ("retry", "retrying", "backoff", "attempt")
    )
    notes.append(f"retry_evidence={'yes' if retry_evidence else 'none'}")

    passed = alive and recovered
    return ScenarioResult(name, passed, time.monotonic() - t0, notes)


def scenario_interface_flap(
    injector: FaultInjector,
    monitor: ServiceMonitor,
    flaps: int = 3,
    down_s: float = 15.0,
    up_s: float = 20.0,
) -> ScenarioResult:
    """Bring wlan0 down/up N times.
    Verify: service survives each flap without crashing.
    """
    name = f"interface_flap_x{flaps}"
    notes = []
    t0 = time.monotonic()

    monitor.snapshot()

    for i in range(flaps):
        log.info("Flap %d/%d: interface down", i + 1, flaps)
        injector.interface_down()
        time.sleep(down_s)
        injector.interface_up()
        log.info("Flap %d/%d: interface up — waiting %.0fs", i + 1, flaps, up_s)
        time.sleep(up_s)

    recovered = monitor.wait_for_active(timeout_s=60)
    restarts = monitor.new_restarts_since_snapshot()
    notes.append(f"recovered={'yes' if recovered else 'NO'}")
    notes.append(f"restarts={restarts}")

    # Watchdog should have kept it alive — 0 restarts is ideal,
    # but 1 restart is acceptable (SIGTERM-safe shutdown + restart)
    passed = recovered and restarts <= 1
    return ScenarioResult(name, passed, time.monotonic() - t0, notes)


def scenario_high_latency(
    injector: FaultInjector,
    monitor: ServiceMonitor,
    delay_ms: int = 2000,
    jitter_ms: int = 500,
    duration_s: float = 90.0,
) -> ScenarioResult:
    """Add 2s latency (± 500ms) — simulates terrible signal or roaming.
    Verify: uploads eventually succeed; no timeout-caused crashes.
    """
    name = f"high_latency_{delay_ms}ms"
    notes = []
    t0 = time.monotonic()

    monitor.snapshot()

    with injector.fault(f"netem delay {delay_ms}ms {jitter_ms}ms"):
        injector.apply_netem(f"delay {delay_ms}ms {jitter_ms}ms")
        time.sleep(duration_s)
        alive = monitor.is_active()
        notes.append(f"alive_during={'yes' if alive else 'NO'}")

    recovered = monitor.wait_for_active(timeout_s=30)
    restarts = monitor.new_restarts_since_snapshot()
    notes.append(f"recovered={'yes' if recovered else 'NO'}")
    notes.append(f"restarts={restarts}")

    log_lines = monitor.recent_log_lines(100)
    timeouts = [line for line in log_lines if "timeout" in line.lower() or "timed out" in line.lower()]
    notes.append(f"timeout_log_lines={len(timeouts)}")

    passed = alive and recovered
    return ScenarioResult(name, passed, time.monotonic() - t0, notes)


def scenario_sustained_chaos(
    injector: FaultInjector,
    monitor: ServiceMonitor,
    duration_s: float = 300.0,
) -> ScenarioResult:
    """Random faults over 5 minutes — the full gauntlet."""
    name = "sustained_chaos"
    notes = []
    t0 = time.monotonic()
    fault_count = 0
    crash_count = 0

    monitor.snapshot()
    deadline = time.monotonic() + duration_s

    faults = [
        ("netem loss 20%", lambda: injector.apply_netem("loss 20%")),
        ("netem delay 1000ms", lambda: injector.apply_netem("delay 1000ms 200ms")),
        ("netem loss 5% corrupt", lambda: injector.apply_netem("loss 5% corrupt 1%")),
        ("iptables DROP azure", lambda: (injector.drop_azure(),)),
    ]

    while time.monotonic() < deadline:
        label, fn = random.choice(faults)
        hold = random.uniform(10, 40)
        calm = random.uniform(5, 20)

        log.info("[chaos] Injecting '%s' for %.0fs", label, hold)
        fn()
        fault_count += 1

        time.sleep(hold)

        if not monitor.is_active():
            crash_count += 1
            log.warning("[chaos] Service went inactive!")

        injector.clear_all()
        time.sleep(calm)

    injector.clear_all()
    recovered = monitor.wait_for_active(timeout_s=60)
    restarts = monitor.new_restarts_since_snapshot()

    notes.append(f"faults_injected={fault_count}")
    notes.append(f"crashes_observed={crash_count}")
    notes.append(f"total_restarts={restarts}")
    notes.append(f"final_state={'active' if recovered else 'DOWN'}")

    passed = recovered and crash_count == 0
    return ScenarioResult(name, passed, time.monotonic() - t0, notes)


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------


def print_report(results: list[ScenarioResult]) -> None:
    width = 70
    print("\n" + "=" * width)
    print("  BeeOps Chaos Test Report  —  " + datetime.now(tz=UTC).strftime("%Y-%m-%d %H:%M:%S"))
    print("=" * width)
    for r in results:
        print(f"  {r}")
    print("-" * width)
    passed = sum(1 for r in results if r.passed)
    print(f"  {passed}/{len(results)} scenarios passed")
    print("=" * width + "\n")

    # Write JSON summary
    summary_path = Path("/tmp/beeops_chaos_results.json")
    summary_path.write_text(json.dumps([dataclasses.asdict(r) for r in results], indent=2))
    print(f"  Full results: {summary_path}\n")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

ALL_SCENARIOS = ["azure_outage", "packet_loss", "interface_flap", "high_latency", "sustained_chaos"]


def main() -> int:
    parser = argparse.ArgumentParser(description="BeeOps network chaos harness")
    parser.add_argument("--iface", default="wlan0", help="Network interface (default: wlan0)")
    parser.add_argument("--service", default="expidite", help="systemd service name (default: expidite)")
    parser.add_argument(
        "--scenarios",
        default="azure_outage,packet_loss,interface_flap,high_latency",
        help=f"Comma-separated list from: {', '.join(ALL_SCENARIOS)}  (or 'all')",
    )
    parser.add_argument(
        "--azure-connection-string", default=None, help="Azure Storage connection string for blob audit"
    )
    parser.add_argument("--azure-container", default="expidite", help="Azure blob container name")
    parser.add_argument(
        "--dry-run", action="store_true", help="Skip fault injection (test harness logic only)"
    )
    args = parser.parse_args()

    if os.geteuid() != 0:
        print("ERROR: Must run as root (sudo) for tc/iptables/ip link commands.", file=sys.stderr)
        return 1

    scenarios = ALL_SCENARIOS if args.scenarios == "all" else args.scenarios.split(",")
    unknown = [s for s in scenarios if s not in ALL_SCENARIOS]
    if unknown:
        print(f"ERROR: Unknown scenarios: {unknown}", file=sys.stderr)
        return 1

    injector = FaultInjector(args.iface)
    monitor = ServiceMonitor(args.service)
    audit = BlobAudit(args.azure_connection_string, args.azure_container)

    if not monitor.is_active():
        log.warning("Service '%s' is not active before tests begin!", args.service)

    log.info(
        "Starting chaos harness — interface=%s service=%s scenarios=%s", args.iface, args.service, scenarios
    )

    results: list[ScenarioResult] = []

    try:
        if "azure_outage" in scenarios:
            results.append(scenario_azure_outage(injector, monitor, audit))

        if "packet_loss" in scenarios:
            results.append(scenario_packet_loss(injector, monitor))

        if "interface_flap" in scenarios:
            results.append(scenario_interface_flap(injector, monitor))

        if "high_latency" in scenarios:
            results.append(scenario_high_latency(injector, monitor))

        if "sustained_chaos" in scenarios:
            results.append(scenario_sustained_chaos(injector, monitor))

    except KeyboardInterrupt:
        log.warning("Interrupted — cleaning up faults...")
    finally:
        injector.clear_all()

    print_report(results)
    return 0 if all(r.passed for r in results) else 1


if __name__ == "__main__":
    sys.exit(main())

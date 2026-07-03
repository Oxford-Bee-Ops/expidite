# Manual test plan: outage data retention & managed reboots

Covers the disk spool (`/expidite-spool`), AsyncCloudConnector offline mode, and the graceful
shutdown/reboot paths. Run on real devices; tick items off as they pass.

## Reference values and observation points

| Thing | Value / location |
|---|---|
| Data path | one network attempt per item; ANY failure → disk spool; the 60s drain is the only retry |
| Offline-mode fault (telemetry only) | after 10 min of continuous transient failures (`SPOOL_OFFLINE_AFTER_SECONDS = 600`) |
| Drain interval | 60s (`SPOOL_DRAIN_INTERVAL`); first item of each pass is the connectivity probe |
| Poison-item quarantine | after 5 non-transient drain failures → `/expidite-spool/quarantine/` |
| Memory divert threshold | 80% (`SPOOL_AT_MEMORY_PERCENT`), RPi only; managed reboot at 95% (`REBOOT_AT_MEMORY_PERCENT`) |
| Spool budget | 16 GB or 1 GB free-disk floor (`SPOOL_MAX_BYTES`, `SPOOL_MIN_DISK_FREE_BYTES`) |
| Shutdown | never touches the network; queue spilled to spool, drained after next start |
| Reboot flush wait | up to 240s (`_REBOOT_FLUSH_TIMEOUT_SECONDS` in `core/reboot.py`) |
| Spool location | `/expidite-spool/upload/<container>/<TIER>/…` and `/expidite-spool/append/<container>/<blob>/…` |
| Logs | `journalctl -t EXPIDITE -f` (or `-u expidite`) |

**Simulating an outage:** cut the *internet* while keeping the LAN up (turn off the router's WAN, or
block outbound 443 upstream). That produces the DNS/connect failures the offline detection classifies
as transient, and - unlike `nmcli radio wifi off` - you keep SSH access to watch it happen. For tests
that deliberately exercise the wifi-recovery / 2h-reboot path, kill the AP itself.

**Tuning constants for tests:** temporarily edit the installed
`~/<venv>/lib/python*/site-packages/expidite_rpi/core/configuration.py` and restart the service.
Revert afterwards (or re-run the installer, which reinstalls the package).

---

## A. Install / upgrade

- [ ] **A1. Upgrade an existing device.** Run `rpi_installer.sh` on a device running the old version.
  - Installer output shows *"Stopping expidite.service gracefully (may take a few minutes to flush
    data)..."* and the stop completes without the SIGKILL fallback warning.
  - `/expidite-spool` exists afterwards, owned by `bee-ops`.
  - `systemctl cat expidite.service` shows `After=time-sync.target network-online.target` and
    `Wants=network-online.target`.
  - Service is running; journal shows normal startup.

- [ ] **A2. Install ending in a reboot.** Trigger an install that sets the reboot flag (e.g. first
  install, or `touch ~/.expidite/flags/reboot_required` before the final steps).
  - Installer prints *"Reboot pending at end of install; Expidite RpiCore will auto-start after the
    reboot."* and does **not** start the service pre-reboot.
  - After the reboot, the service is running (started by systemd/crontab).

- [ ] **A3. Reboot-disabled edge case.** Create both `~/.expidite/flags/reboot_required` and
  `~/.expidite/flags/reboot_disabled`, run the installer.
  - No reboot happens, and the service **is** started (the skip-start must not leave the device dead).

## B. Normal operation (regression)

- [ ] **B1. Healthy device soak (24h).** No outage, normal sensing.
  - `/expidite-spool` stays empty; no offline-mode log lines.
  - HEART rows and sensor files arrive in Azure as before.
  - `bcli stop` / `bcli start` behave as before; stop completes in the usual ~1-3 min.

- [ ] **B2. Graceful stop timing.** With good connectivity, `sudo systemctl stop expidite.service`
  (no bcli).
  - Journal shows *"Received SIGTERM; requesting graceful stop via STOP_EXPIDITE_FLAG"*, then a clean
    stop within `TimeoutStopSec` (240s). No SIGKILL in `journalctl -u expidite`.
  - `systemctl start` afterwards works (stale flag cleared by `start_all()`).

## C. Wifi outage → spool → recovery

- [ ] **C1. Outage behaviour + offline-mode fault.** Cut internet (keep LAN). Wait ~11 min.
  - Files appear under `/expidite-spool/upload/...` (correct container/tier directories) and CSV
    fragments under `/expidite-spool/append/...` **within a minute or two of the cut** - each upload
    fails its one attempt and goes straight to disk.
  - For the first 10 min: *"Temporary network failure: …"* warnings, no customer-facing fault spam.
  - At ~10 min: *"Persistent network failure; device is offline - data is being retained in the disk
    spool at /expidite-spool"* (telemetry only - spooling started immediately).
  - **Critical:** watch `free -m` and `df /expidite` over the next hour - memory and tmpfs usage stay
    roughly flat instead of climbing.

- [ ] **C2. Recovery and drain.** Restore internet before the 2h reboot (e.g. after 60-90 min).
  - Within ~60s (next probe): *"Cloud connectivity restored; leaving offline mode"*, then drain
    activity, ending in *"Disk spool fully drained"*.
  - `/expidite-spool` empties completely (check both `upload/` and `append/` trees).
  - In Azure: HEART/journal CSVs contain rows with timestamps **from the outage period** (this is the
    whole point). Duplicate rows are acceptable; missing rows are a fail.
  - Live sensor data keeps uploading during the drain (drain must not starve it).

- [ ] **C3. The 2-hour outage reboot.** Cut internet and let it run past 2h.
  - Journal shows *"Rebooting device: Rebooting device due to no internet for >2 hours"*, a
    diagnostics bundle collected, then *"RpiCore stopped after Ns; rebooting now"* (N well under 240)
    before the reboot.
  - Shutdown while offline is **fast** - confirm the stop didn't hang on network retries (no 240s
    timeout, no SIGKILL).
  - After reboot (still offline): startup log shows *"Disk spool contains N bytes from a previous
    run"*; spool contents intact; new data continues to spool. The reboot repeats every ~2h while
    offline - spool should accumulate across reboots.
  - Restore internet → full drain as in C2, including data from **before** the first reboot.

## D. Managed reboot paths

For each: confirm the graceful-stop log sequence, that shutdown completes within the flush window, and
that any queued data is in Azure or `/expidite-spool` afterwards - not lost. Verify diagnostics
behaviour matches the table.

| Test | Trigger | Diagnostics bundle expected? |
|---|---|---|
| D1 | `bcli` → reboot (confirm prompt; bcli should visibly wait while the service stops) | No |
| D2 | IoT Hub `reboot` direct method (method response `{"status": "rebooting"}` must arrive) | No |
| D3 | Plain `sudo reboot` from an SSH shell (the uncooperative path - SIGTERM handler must catch it) | No |
| D4 | Wifi >2h (covered by C3) | Yes |
| D5 | Memory >95% (see E2) | Yes |

- [ ] **D1** passed
- [ ] **D2** passed
- [ ] **D3** passed - queue some data first (or run it mid-outage) and verify after boot that the data
  was flushed or spooled; this proves the SIGTERM→flag path works with no helper involved.
- [ ] **D4** passed (C3)
- [ ] **D5** passed (E2)

## E. Memory pressure

- [ ] **E1. Memory divert threshold.** Temporarily set `SPOOL_AT_MEMORY_PERCENT = 20.0` (or run a
  memory hog like `stress-ng --vm 1 --vm-bytes 70%`) with connectivity **up**.
  - Journal: *"Memory usage above X%; diverting queue to disk spool"* (throttled to once a minute);
    queued items go straight to the spool without a network attempt.
  - The spool drains again once pressure drops (or continuously, since we're online - data still
    reaches Azure via the drain).

- [ ] **E2. The 95% reboot (optional, destructive-ish).** Push memory past 95% with a hog.
  - *"Rebooting device: Memory usage >95%, rebooting"* + diagnostics bundle + graceful stop before
    reboot. This should be hard to reach organically now - if it happens during an ordinary outage
    soak, that's a bug in the spill logic.

## F. Video budget

- [ ] **F1. Oldest-video eviction.** On a camera device, temporarily set `SPOOL_MAX_BYTES` to
  something small (e.g. `200 * 1024**2`), cut internet, wait for offline mode, let videos accumulate
  past the budget.
  - Journal: first bin logged as a fault - *"Spool over budget: binned video …"* with RAISE_WARN -
    then quieter per-file INFO lines.
  - Oldest videos disappear from `/expidite-spool/upload/...` first; **CSV/append data is never
    evicted**.
  - Restore internet: surviving videos + all CSVs drain to Azure.

- [ ] **F2. Free-disk floor.** Check `df /` during F1 - free space never drops below ~1 GB.

## G. Resilience edges

- [ ] **G1. Missing spool dir fallback.** On a test device, `sudo rm -rf /expidite-spool`, restart the
  service.
  - Warning *"Spool directory /expidite-spool is not usable"*, then spooling falls back to
    `/expidite-diags/spool` (verify data lands there during an outage). Re-running the installer
    restores `/expidite-spool`.

- [ ] **G2. Watchdog restart unaffected.** `sudo pkill -9 -f <my_start_script>`.
  - systemd restarts the service after ~30s as before; abnormal-restart diagnostics fire; the device
    does not get stuck stopped (a SIGKILL doesn't set the stop flag).

- [ ] **G3. Power cut during outage.** Mid-outage with data spooled, pull power.
  - After boot: spool contents intact, no `.part` debris (cleaned at startup), drain works on
    recovery. Data written in the seconds before the power cut may be lost - that's expected; spooled
    data must not be.

## H. The headline soak test

- [ ] **H1. Multi-day outage.** Cut internet on a full-sensor device for 3-7 days (default budget).
  - Device stays up the whole time (no memory reboots other than the expected 2-hourly wifi-recovery
    reboots).
  - On recovery: all CSV/journal data from the entire outage reaches Azure; videos present up to the
    budget, oldest binned beyond it; binned count visible in the WARNING datastream.

---

**Short on time?** The highest-value tests are **C1-C3** (the core outage story), **D3** (plain
`sudo reboot` is the path that catches everything else), and **H1** before rolling to the fleet.
Do A1 + B1 on one device first as a smoke test before the destructive ones.

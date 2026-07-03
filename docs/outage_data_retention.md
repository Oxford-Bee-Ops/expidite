# Design: outage data retention (disk spool & managed reboots)

Status: implemented July 2026; on-device testing tracked in [outage_test_plan.md](outage_test_plan.md).

## 1. Problem

On an SD-card device, all data awaiting upload lives in RAM:

- The `AsyncCloudConnector` upload queue holds append data (CSV/log lines) directly in process memory;
  `AsyncUpload` items reference files that have been moved into `TMP_DIR`.
- `/expidite` (`ROOT_WORKING_DIR`, containing `TMP_DIR`, `EDGE_UPLOAD_DIR` and the staging journals) is
  mounted as a tmpfs by the installer to protect the SD card from write wear - so "on disk" files are
  also RAM.

During a wifi outage, failed uploads were re-queued indefinitely, so the backlog grew without bound.
The failure sequence was:

1. Outage starts; queue and tmpfs grow.
2. Memory usage passes 95%; `DeviceHealth` reboots the device as a recovery action.
3. The reboot wipes the tmpfs and kills the process. **All queued data is lost.**

Even without memory exhaustion, `DeviceManager` reboots after 2 hours of no internet, losing up to 2
hours of data. And no reboot path flushed anything: `RpiCore` deliberately swallowed SIGTERM (graceful
stop was flag-file driven), so `sudo reboot` meant systemd waited `TimeoutStopSec=240` and then
SIGKILLed the process with the queue still in RAM.

## 2. Goals

- Survive multi-day wifi outages without losing CSV/journal data, and without exhausting RAM.
- Make every deliberate reboot/stop path flush in-flight data first.
- Prefer sacrificing videos (orders of magnitude larger) over any other data when disk space runs out.
- Keep SD-card wear negligible in normal (online) operation.
- Accept rare duplicate CSV rows rather than build exactly-once machinery.

## 3. Architecture overview: store-and-forward on failure

```
Sensor data ──► upload queue (RAM) ──► worker: ONE network attempt ──► Azure
                                              │ any failure
                                              ▼
                                 DiskSpool (/expidite-spool, real disk)
                                              │
                          drain thread (the ONLY retry mechanism)
                          every 60s, oldest first ──────────────► Azure
```

Each queued item gets exactly **one** network attempt on a worker thread. On success it's done; on
**any** failure the data is persisted to the **DiskSpool** (`core/cloud_connector/spool.py`) and the
**drain thread** becomes the only retry mechanism. There is no retry-in-RAM, no offline/online switch
in the data path, and shutdown never touches the network - it spills the queue to the spool and exits.

This bounds RAM by construction: during an outage each item transits the queue once and then lives on
disk, so a device can run offline for a week without memory growth, and a reboot loses nothing that
reached the spool. The cost is negligible SD wear (disk writes only happen on upload failures) plus a
small latency cost at shutdown (in-transit data lands in Azure one drain pass after the next start
rather than before the stop).

## 4. DiskSpool

### Location

`/expidite-spool`, created by the installers alongside `/expidite-diags`, always on real storage so it
survives reboots. Per-platform paths are defined in `configuration.py` (`SPOOL_DIR`); on Windows dev
machines it lives inside the per-run working dir so test runs stay isolated. If the directory is
missing/unwritable (e.g. the installer hasn't been re-run since this feature shipped), the spool falls
back to `/expidite-diags/spool`, and as a last resort to a tmpfs path with a RAISE_WARN fault.

### Layout

```
/expidite-spool/
  upload/<container>/<TIER>/<filename>        block-blob uploads; destination container and storage
                                              tier are encoded in the path (no sidecar metadata)
  append/<container>/<blob_name>/<uuid>.csv   append-blob fragments; each a complete CSV with headers
```

- **Uploads** keep their FAIR-compliant filenames, which are unique per record. A name collision in
  the spool therefore means "same record" and is resolved by overwrite - this is what makes the
  shutdown safety-copy (below) idempotent instead of duplicating blobs.
- **Appends** are stored as independent uuid-named fragments rather than appended to one growing file:
  concurrent worker threads never contend, and each fragment can be pushed through the existing
  `append_to_cloud` path unchanged (which drops the header row when the remote blob exists).

### Crash safety

All writes go to a `.part` name and are renamed into place (rename within one filesystem is atomic).
`.part` files are invisible to the drain, so it never uploads a half-file. Debris from a crashed run is
deleted at construction, but only when older than one hour: the spool directory is shared between
processes (the service, bcli, the management service all construct connectors), and a fresh `.part` may
be a live write by one of the others.

### Disk budget

Governed by `SPOOL_MAX_BYTES` (16 GB default) and a hard free-space floor `SPOOL_MIN_DISK_FREE_BYTES`
(1 GB). When an incoming item would breach the budget:

1. The oldest spooled **videos** (`.mp4`/`.avi`/`.h264`, by mtime) are evicted until it fits.
2. If no evictable video remains and the incoming item is itself a video, it is dropped.
3. Non-video data (CSVs, logs) is allowed to overshoot `SPOOL_MAX_BYTES` - it is small and precious -
   bounded only by the free-space floor.

The first dropped video per run is a RAISE_WARN fault (visible in the customer WARNING datastream);
subsequent bins log at INFO with a periodic RAISE_WARN counter, so a week-long outage doesn't produce
thousands of faults.

## 5. The data path in AsyncCloudConnector

### One attempt, then disk

Worker threads (`_async_upload` / `_async_append`) make a single network attempt per item. On any
exception the data is persisted via `_spool_action()`. If the spool itself cannot take the data
(`SpoolResult.FAILED`: disk full or unwritable - distinct from a deliberately BINNED video), the item
falls back to the in-memory queue with a 10s backoff rather than being dropped: this is the
doubly-degraded case (no network AND no disk) and the data cycles in RAM until one of them recovers.

### Offline mode is telemetry only

Every failed cloud call is classified with `is_transient_network_error()`; once transient failures
have been continuous for `SPOOL_OFFLINE_AFTER_SECONDS` (600s - the same constant that drives
`log_cloud_failure`'s fault escalation, by construction), the device reports offline mode as a
customer-facing fault, and clears it on the next successful call. **No data path depends on this
state** - behavior is identical online and offline, which removes the largest source of state-machine
complexity from the previous design.

### Memory pressure

Normally RAM is bounded because each item transits the queue once. The exception is a black-holing
network: workers stuck in long connection timeouts while the queue backs up behind them. If memory
exceeds `SPOOL_AT_MEMORY_PERCENT` (80%, RPi only), `do_work` sends dequeued items straight to disk
without a network attempt until pressure clears. `DeviceHealth`'s managed reboot fires at
`REBOOT_AT_MEMORY_PERCENT` (95%) - both constants live side by side in `configuration.py` with the
ordering invariant documented.

### Draining - the only retry mechanism

A dedicated daemon thread wakes every `SPOOL_DRAIN_INTERVAL` (60s) and drains sequentially - appends
first (small CSVs), oldest first - so a large backlog trickles out without starving live uploads of
the worker pool:

- The **first item of a pass doubles as the connectivity probe**: a transient network failure aborts
  the pass, to be retried next tick.
- A **non-transient** per-item failure (e.g. a destination append blob at Azure's 50,000-block limit,
  or a deleted container) is counted and skipped; after 5 failed attempts the item is **quarantined**
  to `<spool>/quarantine/` - preserved for manual recovery, invisible to the drain - so one poison
  item can never wedge everything spooled behind it.
- A spooled file that has vanished (evicted for budget, or drained by another process's connector) is
  tidied away without being treated as network evidence.

The same thread handles the **startup drain**: if the spool holds data when the connector is
constructed (i.e. after a reboot), the drain is armed immediately. There is no "reload the queue into
RAM" step - spooled data goes disk → Azure directly, while new data flows through the normal path.

### Idempotency / duplicates

- Re-uploading a block blob is a harmless overwrite (`overwrite=True`, same FAIR name).
- For appends there is a small window (crash between a successful `append_block` and deleting the
  fragment) that produces duplicate CSV rows. This is a deliberate trade-off: duplicates are
  analytically harmless and detectable via `RECORD_ID`, and exactly-once semantics would require a
  transaction log.

## 6. Flush-safe shutdown

`AsyncCloudConnector.shutdown()` never touches the network, so it is fast and bounded regardless of
connectivity:

1. Everything still queued is spilled to the spool (`do_work` spools anything it has already dequeued
   once stop is requested, so nothing is stranded between the queue and the worker pool).
2. The data of any upload still running on a worker thread is *safety-copied* to the spool: if the
   process is killed mid-upload, the data is on disk rather than lost with the tmpfs; if the upload
   completes anyway, the next drain re-uploads the same blob name (harmless overwrite / accepted
   duplicate rows).

Data in transit at shutdown therefore lands in Azure one drain pass after the next start instead of
before the stop - the deliberate price of a stop path with no timing windows, no network flush
deadline, and no online/offline branching.

**Residual risk:** a worker thread stuck deep in a long Azure connection timeout cannot be cancelled;
process exit may then block until systemd's SIGKILL at `TimeoutStopSec=240`. By that point its data
has already been safety-copied, so nothing is lost - the device just reboots a little slower.

### SIGTERM

`RpiCore` translates SIGTERM into touching `STOP_EXPIDITE_FLAG` (previously it was swallowed). The
flag remains the single source of truth for "stop": the EdgeOrchestrator main loop polls it every
second and runs the full graceful stop (sensors stop → journals flush → connector shutdown as above).
This makes `systemctl stop`, `systemctl restart` and - critically - the system-wide SIGTERM that
systemd sends during **any** `sudo reboot` flush-safe, even ones that bypass our own tooling. A stale
flag cannot block startup because `EdgeOrchestrator.start_all()` clears it.

The handler is installed by `RpiCore.start()`, not `__init__`: bcli and the management service also
construct `RpiCore` (for configure/status), and installing the handler in those processes would make a
SIGTERM to them stop the main data-collection service as collateral damage. Only the process that runs
the orchestrator owns the handler.

The service units also gained `After=network-online.target` / `Wants=network-online.target`: unit stop
order is the reverse of start order, so during a reboot expidite stops *before* the network is torn
down - journal flushes that are already in flight can still complete.

## 7. Managed reboots

All deliberate reboots go through `request_managed_reboot(reason)` in `core/reboot.py`:

1. Optionally collect a diagnostics bundle (see table).
2. Touch `STOP_EXPIDITE_FLAG` and wait (bounded, 240s) for `EXPIDITE_IS_RUNNING_FLAG` to be *removed* -
   the orchestrator's main loop deletes it only after `stop_all()` has finished flushing, so removal is
   a positive "fully stopped" signal. (`watchdog_file_alive()` cannot be used here: it reports False the
   moment the STOP flag is newer than the running flag, minutes before the flush completes.)
3. `sudo reboot`.

By default the wait runs on a daemon thread (`background=True`). This is required for callers that run
on threads `stop_all()` joins (the device manager, health checks): blocking such a thread on the
shutdown it just triggered would deadlock. BCLI - a separate process - uses `background=False` and
blocks; the flag files are shared via the filesystem, so the mechanism works cross-process.

| Caller | Diagnostics bundle? |
|---|---|
| Wifi outage >2h (`device_manager.py`) | Yes - fault recovery |
| Memory >95% (`device_health.py`) | Yes - fault recovery |
| Stale HEART file (`device_health.py`) | Yes - fault recovery |
| IoT Hub `reboot` direct method | No - user requested, nothing to diagnose |
| BCLI reboot | No - user requested |

### Installer behaviour

- `stop_expidite_service()` in both installers now does a graceful `systemctl stop` first (systemd
  escalates to SIGKILL itself after `TimeoutStopSec`); the explicit SIGKILL remains only as a fallback
  if the stop fails outright.
- If an installer run ends in a reboot (`reboot_required` flag set and reboots not disabled by the
  cyclical-reboot guard), `auto_start_if_requested()` installs and enables the services but does not
  start them - they would only be torn down again seconds later. They start on the post-reboot boot.
  The skip condition deliberately mirrors `reboot_if_required()` exactly, so a device with reboots
  disabled still gets its services started.

## 8. Configuration

All tuning constants live in `core/configuration.py` for easy review and mocking:

| Constant | Default | Meaning |
|---|---|---|
| `SPOOL_OFFLINE_AFTER_SECONDS` | 600 | Continuous transient failure before reporting offline mode (telemetry; also drives fault escalation) |
| `SPOOL_DRAIN_INTERVAL` | 60 | Seconds between drain passes (first item of a pass is the connectivity probe) |
| `SPOOL_AT_MEMORY_PERCENT` | 80 | Memory usage above which dequeued items go straight to disk (RPi only) |
| `REBOOT_AT_MEMORY_PERCENT` | 95 | DeviceHealth managed-reboot threshold; must stay above `SPOOL_AT_MEMORY_PERCENT` |
| `SPOOL_MAX_BYTES` | 16 GB | Spool size budget (videos evicted oldest-first beyond it) |
| `SPOOL_MIN_DISK_FREE_BYTES` | 1 GB | Free-disk floor spooling never crosses |
| `SPOOL_DIR` | `/expidite-spool` | Spool root (per-platform) |

## 9. What this changes in practice

| Scenario | Before | After |
|---|---|---|
| Short outage (<10 min) | Retried in RAM, no loss | Data goes to disk on first failure, drained on recovery - no loss |
| Long outage, memory fills | Reboot at 95% → total loss | RAM never grows: one transit per item, then disk; device keeps running |
| Wifi >2h reboot | Up to 2h of data lost | Flushed to spool pre-reboot; drained on recovery |
| `sudo reboot` / `systemctl stop` | SIGTERM ignored → SIGKILL → loss | Graceful stop, flush/spool |
| Installer upgrade | SIGKILL first → loss | Graceful stop first |
| Week-long outage | Device unusable, total loss | CSVs retained indefinitely; videos kept up to 16 GB, then dropped oldest-first |

## 10. Testing

- Unit tests: `test/rpi_core/core/disk_spool_test.py` - spool round-trips, budget eviction, offline
  state machine, drain, shutdown spill and startup drain, all with network methods monkeypatched (no
  Azure required).
- Manual/field: [outage_test_plan.md](outage_test_plan.md).

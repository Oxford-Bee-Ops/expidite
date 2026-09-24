# Boot history

The boot history is a small, best-effort record of hardware boots, requested reboots and orderly shutdowns.
It is stored on persistent storage at:

```text
/expidite-diags/boot_history.jsonl
```

Each event is one compact JSON object on one line, written by `src/expidite_rpi/scripts/boot_history.py`. The
recorder only ever appends; it never reads the file back.

## Event formats

Every event starts with a UTC timestamp `at` to the second (such as `2026-09-24T09:17:31`), the kernel
`boot_id` of the boot it happened in, and an `event` type. Any other fields follow. The `boot` event is written
before NTP has necessarily synced, so its `at` may be stale: Pi 4 and Pi Zero models have no real-time clock,
and a Pi 5 RTC without a battery loses the time on power loss. After an unexpected reset, `at` is then the last
saved clock time and can be earlier than events already in the file. Rely on file order, not `at`, to sequence
events. In the examples below, `A` is the old kernel boot ID and `B` is the new one.

Boot event, written once early in each boot:

```json
{"at": "<time>", "boot_id": "B", "event": "boot"}
```

Raspberry Pi 5 boot events can additionally contain the PMIC reset bitfield and its decoded flags:

```json
"power_reset": {"raw": 2, "flags": ["under_voltage"]}
```

Reboot request event, written by whoever asks for a reboot, immediately before it issues `sudo reboot`:

```json
{"at": "<time>", "boot_id": "A", "event": "reboot_requested", "reason": "<reason>"}
```

Orderly shutdown event, written during every orderly reboot or poweroff:

```json
{"at": "<time>", "boot_id": "A", "event": "shutdown"}
```

`reason` is truncated to 500 characters.

## Who writes events

| Event | Written by |
|---|---|
| `boot` | The `expidite-boot-history` systemd service runs `boot_history.py record-boot` when it starts, before `expidite.service`. |
| `shutdown` | The same service runs `boot_history.py record-shutdown` when it stops. systemd stops it during every reboot or poweroff. |
| `reboot_requested` | `reboot.py` (internal health recovery, BCLI and IoT Hub reboots) calls `record_reboot_request()`. The installers run `boot_history.py record-request "<reason>"`. |

The installers create the service, and its `ExecStart` and `ExecStop` run as `bee-ops`.

## Reading the history

Group the events by `boot_id` and read each boot's outcome from what it contains:

| Events for boot `A` | Meaning |
|---|---|
| `boot`, `shutdown` | Orderly shutdown that nobody requested through Expidite, e.g. a plain `sudo reboot` or `sudo poweroff`. |
| `boot`, `reboot_requested`, `shutdown` | Orderly requested reboot. The `reason` says why. |
| `boot`, `reboot_requested` | A reboot was requested, but boot `A` never shut down cleanly: the shutdown hung and was reset, or power was lost part-way through. If no later boot follows for a long time, the `sudo reboot` command itself failed. |
| `boot` only | Boot `A` ended without a recorded shutdown: power loss, kernel panic, watchdog reset, hard reset or another abrupt failure. |

Readers should also:

- **Ignore duplicate lines.** For simplicity, the recorder does not suppress duplicates.
  If a hook runs twice in one kernel boot, a second line with the same `event` and `boot_id` appears.
- **Use the last `reboot_requested` in a boot.** If a reboot request fails and a later one succeeds within the
  same boot, there are two; the later one is the reason for the shutdown.
- **Skip lines that are not valid JSON.** See the power-loss row below.

## Rotation

When the active file reaches 100 KB (roughly 500 events), it is moved to `boot_history.1.jsonl` immediately
before the next `boot` event is recorded. The previous backup is replaced, so storage is bounded to two files
of about 100 KB each. Rotation happens only at a boot boundary, so all events for one boot stay in the same
file.

## Reboots and orderly shutdowns

| Scenario | History entries for the old boot `A` |
|---|---|
| First boot ever observed | A single `boot` event. |
| Plain `sudo reboot` | `shutdown`, followed by the next `boot`. |
| Plain `sudo poweroff`, followed by a later power-on | The same as a plain reboot. The history does not record how long the device remained off. |
| `rpi_installer` reboot | `reboot_requested` with reason `rpi_installer requested reboot #<n>`, then `shutdown`. |
| `zero_installer` reboot | `reboot_requested` with reason `zero_installer requested reboot #<n>`, then `shutdown`. |
| BCLI reboot | `reboot_requested` with reason `service stop and reboot: Reboot requested via BCLI`, then `shutdown`. |
| IoT Hub reboot | `reboot_requested` with reason `service stop and reboot: Reboot requested via IoT Hub`, then `shutdown`. |
| Internal health recovery | `reboot_requested` with reason `managed reboot: <specific fault reason>`, then `shutdown`. |
| Manually stamped command-line reboot | Run `boot_history.py record-request "<reason>"` before `sudo reboot`: `reboot_requested` with exactly that reason, then `shutdown`. |
| `sudo reboot` fails | `reboot_requested` with no `shutdown` after it. For BCLI and IoT Hub reboots, Expidite is also restarted. |
| Repeated boot-hook or shutdown-hook invocation | A duplicate line with the same `event` and `boot_id`. Readers ignore it. |
| Boot-history service stopped while the device keeps running | A `shutdown` event is still written, although the device did not shut down. systemd does not tell the hook why it is being stopped. This is rare: `RefuseManualStop=yes` blocks `systemctl stop`, and `/expidite-diags` is not unmounted in normal operation. |

A successful BCLI reboot produces these lines:

```json
{"at": "<time>", "boot_id": "A", "event": "reboot_requested", "reason": "service stop and reboot: Reboot requested via BCLI"}
{"at": "<time>", "boot_id": "A", "event": "shutdown"}
{"at": "<time>", "boot_id": "B", "event": "boot"}
```

## Unexpected resets

| Scenario | History entries |
|---|---|
| Sudden power loss | Nothing can be written when power disappears. Boot `A` has no `shutdown`; the next event is `boot B`. |
| Kernel panic | Normally indistinguishable from power loss. |
| Hard reset or reset button | Boot `A` has no `shutdown`. |
| Frozen OS followed by an external reset | Boot `A` has no `shutdown`. |
| Watchdog reset | Boot `A` has no `shutdown`; on a Pi 5, `boot B` may report the `watchdog` PMIC flag. |
| Brownout or under-voltage reset | Boot `A` has no `shutdown`; on a Pi 5, `boot B` may report the `under_voltage` PMIC flag. |
| Reset during a requested reboot | Boot `A` has `reboot_requested` but no `shutdown`. |
| Power lost while writing an event | Very unlikely: each event is one small write followed by `fsync`. A partial final line may remain, and the next event is appended to it, so both lines are invalid JSON. Readers skip invalid lines. |

An unexpected reset without additional evidence looks like:

```json
{"at": "<time>", "boot_id": "A", "event": "boot"}
{"at": "<time>", "boot_id": "B", "event": "boot"}
```

A Pi 5 may provide supporting PMIC evidence:

```json
{"at": "<time>", "boot_id": "B", "event": "boot", "power_reset": {"raw": 2, "flags": ["under_voltage"]}}
```

The PMIC flags remain evidence; for example, `under_voltage` does not by itself prove a sudden power loss.

## Failure behaviour

The history is deliberately silent and best-effort. It has no application imports or logger setup, and a
failure to write diagnostic evidence never prevents startup, shutdown, service recovery or reboot.

import subprocess

from expidite_rpi.core import api, file_naming
from expidite_rpi.core import configuration as root_cfg

logger = root_cfg.setup_logger("expidite")

DIAGNOSTIC_COMMANDS = [
    # System and time.
    ("System Date & Time", "date"),
    ("System Uptime & Load", "uptime"),
    ("Kernel Information", "uname -a"),
    # Network status.
    ("All Network Interfaces", "ip a"),
    ("Routing Table", "ip r"),
    ("ARP Cache", "arp -a"),
    ("Active Connections", "ss -tulnpa"),
    ("Wi-Fi Status (if on older systems)", "iwconfig"),
    ("DNS Resolution Config", "cat /etc/resolv.conf"),
    # Connectivity checks.
    ("Ping Google DNS (8.8.8.8)", "ping -c 4 8.8.8.8"),
    # Resource usage.
    ("Disk Usage", "df -h"),
    ("Memory Usage", "free -h"),
    ("Top Processes Snapshot", "top -bn1 | head -n 20"),  # Only show the first 20 lines
    ("Last 50 Kernel Messages", "dmesg | tail -n 50"),
    # Logs.
    ("Recent logs", "journalctl -n 500 --no-pager"),
]

##############################################################################################################
# The sole purpose of this class is to write a diagnostics bundle to disk, containing as much information as
# possible to help understand how/why the device got into a bad state, such as lost connectivity or out of
# memory. Any code which deliberately reboots the device should first generate a diagnostics bundle.
#
# The diagnostics bundle is expected to be retrieved either:
# - manually by copying off the SD card, if the device never recovers and has to be physically retrieved, or
# - if connectivity recovers, it will be uploaded to cloud storage.
##############################################################################################################
class DeviceStatus:
    @staticmethod
    def collect_diagnostics(reason: str):
        """Collects and saves a diagnostics bundle to a time-stamped file."""

        if not root_cfg.running_on_rpi:
            return

        log_filename = file_naming.get_diags_filename()

        logger.info(f"Starting diagnostic collection to {log_filename}")

        with open(log_filename, "w") as f:
            f.write("=" * 120 + "\n")
            f.write(f"Report generated: {api.utc_now()}\n")
            f.write(f"Reason:           {reason}\n")
            f.write("=" * 120 + "\n\n")

            for title, command in DIAGNOSTIC_COMMANDS:
                f.write(f"### {title} ({command}) ###\n")
                stdout, stderr, returncode = DeviceStatus.run_cmd(command)
                f.write(f"Exit Code: {returncode}\n")

                if stdout:
                    f.write("--- STDOUT ---\n")
                    f.write(stdout + "\n")

                if stderr:
                    f.write("--- STDERR ---\n")
                    f.write(stderr + "\n")

                f.write("\n" + "=" * 120 + "\n\n")

        logger.info(f"Completed diagnostic collection to {log_filename}")

    @staticmethod
    def run_cmd(command) -> tuple[str, str, int]:
        """Executes a shell command and returns its output and any errors."""
        try:
            result = subprocess.run(
                command,
                shell=True,
                capture_output=True,
                text=True,
                timeout=15, # Timeout in seconds, in case any command hangs.
            )
            return result.stdout.strip(), result.stderr.strip(), result.returncode
        except subprocess.TimeoutExpired:
            return "COMMAND TIMEOUT: Command exceeded 15 seconds.", "", 1
        except Exception as e:
            return f"EXECUTION ERROR: {e}", "", 1

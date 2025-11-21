import gzip
import subprocess

from expidite_rpi.core import api, file_naming
from expidite_rpi.core import configuration as root_cfg

logger = root_cfg.setup_logger("expidite")

DIAGNOSTIC_COMMANDS = [
    # System and time.
    ("System date and time", "date"),
    ("System uptime", "uptime"),
    ("Kernel information", "uname -a"),
    # Network status.
    ("Network Manager status", "nmcli general status"),
    ("Network interface status", "nmcli device status"),
    ("Configured connections", "nmcli connection show"),
    ("Wi-Fi hardware state", "nmcli radio"),
    ("All network interfaces", "ip a"),
    ("Routing table", "ip r"),
    ("ARP cache", "arp -a"),
    ("Active connections", "ss -tulnpa"),
    ("Wi-Fi status (if on older systems)", "iwconfig"),
    ("DNS resolution config", "cat /etc/resolv.conf"),
    # Connectivity checks.
    ("Ping Google DNS (8.8.8.8)", "ping -c 4 8.8.8.8"),
    # Power and hardware Status
    ("CPU Temperature", "vcgencmd measure_temp"),
    ("Voltage/throttling status", "vcgencmd get_throttled"),
    ("Current CPU/GPU clock speeds", "vcgencmd measure_clock arm; vcgencmd measure_clock core"),
    # Resource usage.
    ("Disk usage", "df -h"),
    ("Memory usage", "free -h"),
    ("Top processes snapshot", "top -bn1 | head -n 20"),  # Only show the first 20 lines
    ("Last 50 kernel messages", "dmesg | tail -n 50"),
    # Logs.
    ("Recent logs", "journalctl -n 1000 --no-pager"),
    # Expidite status
    ("Expidite files", f"ls -lhR {root_cfg.ROOT_WORKING_DIR}"),
    ("Expidite config files", f"ls -lhR {root_cfg.CFG_DIR}"),
    ("EXPIDITE_IS_RUNNING_FLAG", f"cat {root_cfg.EXPIDITE_IS_RUNNING_FLAG}"),
    ("LED_STATUS_FILE", f"cat {root_cfg.LED_STATUS_FILE}"),
]

##############################################################################################################
# The sole purpose of this class is to write a diagnostics bundle to disk, containing as much information as
# possible to help understand how/why the device got into a bad state, such as lost connectivity or out of
# memory. Any code which deliberately reboots the device should first generate a diagnostics bundle.
#
# The diagnostics bundle is expected to be retrieved either:
# - manually by copying off the SD card, if the device never recovers and has to be physically retrieved, or
# - if connectivity recovers, it will be uploaded to cloud storage.
#
# There is a small amount of duplication here with DeviceHealth, RpiCore and bcli. This is hard to avoid while
# keeping this class self contained.
##############################################################################################################
class DiagnosticsBundle:
    @staticmethod
    def collect(reason: str):
        """Collects and saves a diagnostics bundle to a time-stamped file."""

        if not root_cfg.running_on_rpi:
            return

        log_filename = file_naming.get_diags_filename()

        logger.info(f"Starting diagnostic collection to {log_filename}")

        def write_bar():
            f.write("=" * 150 + "\n")

        with gzip.open(log_filename, "wt") as f:
            write_bar()
            f.write(f"Report generated: {api.utc_now()}\n")
            f.write(f"Reason:           {reason}\n")
            write_bar()

            for title, command in DIAGNOSTIC_COMMANDS:
                f.write(f"\n### {title} ({command}) ###\n")
                stdout, stderr, returncode = DiagnosticsBundle.run_cmd(command)
                f.write(f"Exit code: {returncode}\n")

                if stdout:
                    f.write("--- STDOUT ---\n")
                    f.write(stdout + "\n")

                if stderr:
                    f.write("--- STDERR ---\n")
                    f.write(stderr + "\n")

                f.write("\n")
                write_bar()

            expidite_version, user_code_version = root_cfg.get_version_info()
            f.write(f"\nExpidite version: {expidite_version}\n")
            f.write(f"User code version: {user_code_version}\n")
            f.write("\nExpidite system configuration:\n")
            for key, value in root_cfg.system_cfg.model_dump().items():
                f.write(f"    {key}: {value}\n")

            f.write(f"\nExpidite device configuration:\n{root_cfg.my_device.display()}")
            if root_cfg.keys:
                f.write(f"\nStorage account: {root_cfg.keys.get_storage_account()}\n")

            # Import here to avoid circular dependency.
            from expidite_rpi.core.device_health import DeviceHealth
            health = DeviceHealth().get_health(check_memory_usage = False)
            if health:
                f.write("\nDevice health\n")
                for key, value in health.items():
                    f.write(f"    {key}: {value}\n")

            f.write("\n")
            write_bar()

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

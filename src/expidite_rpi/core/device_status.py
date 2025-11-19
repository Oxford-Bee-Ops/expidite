import subprocess
import os
import datetime
import platform
import sys

from expidite_rpi.core import configuration as root_cfg

logger = root_cfg.setup_logger("expidite")

# NICKB Replace with the correct location.
LOG_DIR = "/expidite/nick-test"

# List of diagnostic commands to execute. Each command's output will be logged.
# Commands are tailored for standard Linux environments like Raspberry Pi OS.
DIAGNOSTIC_COMMANDS = [
    # System & Time
    ("System Date & Time", "date"),
    ("System Uptime & Load", "uptime"),
    ("Kernel Information", "uname -a"),
    # Network Status
    ("All Network Interfaces (ip a)", "ip a"),
    ("Routing Table (ip r)", "ip r"),
    ("ARP Cache", "arp -a"),
    ("Active Connections (ss)", "ss -tulnpa"),
    ("Wi-Fi Status (if on older systems)", "iwconfig"),
    ("DNS Resolution Config", "cat /etc/resolv.conf"),
    # Connectivity Checks
    ("Ping Google DNS (8.8.8.8)", "ping -c 4 8.8.8.8"),
    # Resource Usage
    ("Disk Usage (df)", "df -h"),
    ("Memory Usage (free)", "free -h"),
    ("Top Processes Snapshot", "top -bn1 | head -n 20"),  # Only show the first 20 lines
    ("Last 50 Kernel Messages", "dmesg | tail -n 50"),
    # Log Snippets (Requires sudo or specific user permissions)
    # Uncomment the following if you run the script with permissions (e.g., via root cron job)
    # ("Recent Journal Logs (Network/Systemd)", "journalctl -u systemd-networkd -u dhcpcd -n 50 --no-pager"),
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
        """Collects and saves all diagnostic outputs to a time-stamped file."""

        # NICKB use expidite version
        if platform.system() != "Linux":
            logger.info(
                f"Error: This script is designed for Linux (Raspberry Pi). Current OS is {platform.system()}."
            )
            sys.exit(1)

        # Ensure log directory exists
        os.makedirs(LOG_DIR, exist_ok=True)

        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        log_filename = os.path.join(LOG_DIR, f"diag_{timestamp}.txt")

        logger.info(f"Starting diagnostic collection. Log file: {log_filename}")

        with open(log_filename, "w") as f:
            f.write("--- Expidite connectivity diagnostics ---------------------------------------------\n")
            f.write(f"Report Generated: {datetime.datetime.now().isoformat()}\n")
            f.write(f"Reason: {reason}\n")
            f.write(f"Log Location: {log_filename}\n")
            f.write("-----------------------------------------------------------------------------------\n\n")

            for title, command in DIAGNOSTIC_COMMANDS:
                f.write(f"### {title} (Command: {command}) ###\n")
                stdout, stderr, returncode = DeviceStatus.execute_command(command)
                f.write(f"Exit Code: {returncode}\n")

                if stdout:
                    f.write("--- STDOUT ---\n")
                    f.write(stdout + "\n")

                if stderr:
                    f.write("--- STDERR (Potential Errors) ---\n")
                    f.write(stderr + "\n")

                f.write("\n" + "=" * 80 + "\n\n")

        logger.info(f"\nDiagnostic collection complete. Data saved to: {log_filename}")

    # NICKB: use an existing util function?
    @staticmethod
    def execute_command(command):
        """Executes a shell command and returns its output and any errors."""
        try:
            # Using shell=True for complex commands/pipes, but generally discouraged.
            # For simplicity in this diagnostic script, we use it for pipes/redirects.
            result = subprocess.run(
                command,
                shell=True,
                capture_output=True,
                text=True,
                timeout=15,  # Timeout in seconds for any single command
            )
            return result.stdout.strip(), result.stderr.strip(), result.returncode
        except subprocess.TimeoutExpired:
            return "COMMAND TIMEOUT: Command exceeded 15 seconds.", "", 1
        except Exception as e:
            return f"EXECUTION ERROR: {e}", "", 1

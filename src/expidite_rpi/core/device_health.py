import os
import socket
import subprocess
from datetime import datetime
from typing import Any

import psutil

from expidite_rpi.core import api
from expidite_rpi.core import configuration as root_cfg
from expidite_rpi.core.device_manager import DeviceManager
from expidite_rpi.core.diagnostics_bundle import DiagnosticsBundle
from expidite_rpi.core.dp_config_objects import SensorCfg, Stream
from expidite_rpi.core.sensor import Sensor
from expidite_rpi.utils import utils

if root_cfg.running_on_rpi:
    from systemd import journal  # type: ignore

    def get_logs(
        since: datetime | None = None,
        min_priority: int | None = None,
        grep_str: list[str] | None = None,
        max_logs: int = 1000,
    ) -> list[dict[str, Any]]:
        """
        Fetch logs from the system journal.

        Args:
            since (datetime): A timestamp to fetch logs since.
            min_priority (int): The priority level (e.g., 6 for informational, 4 for warnings).
            grep_str (list[str]): List of strings to filter log messages.
            max_logs (int): Maximum number of logs to fetch.

        Returns:
            list[dict[str, Any]]: A list of log entries.
        """
        logs: list[dict] = []
        try:
            reader = journal.Reader()
        except Exception as e:
            logger.error(f"{root_cfg.RAISE_WARN()}Failed to initialize journal reader: {e}")
            return logs

        # Set filters
        if since:
            if isinstance(since, datetime):
                reader.seek_realtime(since.timestamp())
            else:
                raise ValueError("The 'since' argument must be a datetime object.")

        # Iterate through the logs
        for entry in reader:
            priority = int(entry.get("PRIORITY", 9))
            message = entry.get("MESSAGE", "")
            if (min_priority is None or priority <= min_priority or api.RAISE_WARN_TAG in message) and (
                grep_str is None or all(s in message for s in grep_str)
            ):
                time_logged: datetime = entry.get("__REALTIME_TIMESTAMP")
                log_entry = {
                    "time_logged": time_logged,
                    "message": entry.get("MESSAGE", "No message"),
                    "process_id": entry.get("_PID"),
                    "process_name": entry.get("_COMM"),
                    "executable_path": entry.get("_EXE"),
                    "priority": entry.get("PRIORITY"),
                }
                logs.append(log_entry)
                if len(logs) >= max_logs:
                    break
        logger.info(f"Fetched {len(logs)} logs from the journal.")

        return logs


logger = root_cfg.setup_logger("expidite")

# HEART - special datastream for recording device & system health
HEART_FIELDS = [
    "boot_time",
    "last_update_time",
    "cpu_percent",
    "total_memory_gb",
    "memory_percent",
    "memory_free",
    "disk_percent",
    "disk_bytes_written_in_period",
    "io_bytes_sent",
    "expidite_mount_size",
    "expidite_mount_percent",
    "cpu_temperature",
    "ssid",
    "signal_strength",
    "packet_loss",
    "current_ping_fail_run",
    "ip_address",
    "power_status",
    "process_list",
    "expidite_version",
    "user_code_version",
]

# WARNING - special datastream for capturing warning and error logs from any component
WARNING_FIELDS = [
    "time_logged",
    "message",
    "process_id",
    "process_name",
    "executable_path",
    "priority",
]

HEART_STREAM_INDEX = 0
WARNING_STREAM_INDEX = 1
DEVICE_HEALTH_CFG = SensorCfg(
    sensor_type=api.SENSOR_TYPE.SYS,
    sensor_index=0,
    sensor_model="DeviceHealth",
    description="Internal device health",
    outputs=[
        Stream(
            "Health heartbeat stream",
            api.HEART_DS_TYPE_ID,
            HEART_STREAM_INDEX,
            format=api.FORMAT.LOG,
            fields=HEART_FIELDS,
            cloud_container=root_cfg.my_device.cc_for_system_records,
        ),
        Stream(
            "Warning log stream",
            api.WARNING_DS_TYPE_ID,
            WARNING_STREAM_INDEX,
            format=api.FORMAT.LOG,
            fields=WARNING_FIELDS,
            cloud_container=root_cfg.my_device.cc_for_system_records,
        ),
    ],
)


class DeviceHealth(Sensor):
    """Monitors device health and provides data as a RpiCore datastream.
    Produces the following data:
    - HEART (DS type ID) provides periodic heartbeats with device health data up to cloud storage.
    - WARNINGS (DS type ID) captures warning and error logs produced by any component, aggregates
      them, and sends them up to cloud storage.
    """

    def __init__(self, device_manager: DeviceManager | None = None) -> None:
        super().__init__(DEVICE_HEALTH_CFG)
        ######################################################################################################
        # Telemetry tracking
        ######################################################################################################
        self.last_ran = api.utc_now()
        self.device_id = root_cfg.my_device_id
        self.cum_bytes_written = 0
        self.cum_bytes_sent = 0
        self.last_ping_success_count_all = 0
        self.last_ping_failure_count_all = 0
        self.log_counter = 0
        self.device_manager = device_manager

    def run(self) -> None:
        """Main loop for the DeviceHealth sensor.
        This method is called when the thread is started.
        It runs in a loop, logging health data and warnings at regular intervals.
        """
        try:
            logger.info(f"Starting DeviceHealth thread {self!r}")

            while not self.stop_requested.is_set():
                # Log the health data
                self.log_health()

                # Log the warning data
                self.log_warnings()

                # Set timer for next run
                self.last_ran = api.utc_now()
                self.log_counter += 1
                sleep_time = root_cfg.my_device.heart_beat_frequency
                self.stop_requested.wait(sleep_time)
        except Exception as e:
            logger.error(f"{root_cfg.RAISE_WARN()}Error in DeviceHealth thread: {e}", exc_info=True)

    def log_health(self) -> None:
        """Logs device health data to the HEART datastream."""
        health = self.get_health()
        self.log(HEART_STREAM_INDEX, health)

    def log_warnings(self) -> None:
        """Capture warning and error logs to the WARNING datastream.
        We get these from the system journal and log them to the WARNING datastream.
        We capture logs tagged with the RAISE_WARN_TAG and all logs with priority <=3 (Error)."""
        if root_cfg.running_on_rpi:
            logs = get_logs(since=self.last_ran, min_priority=4)
            self.last_ran = api.utc_now()

            for log in logs:
                if str(log["message"]).startswith(api.RAISE_WARN_TAG):
                    log["priority"] = int(log.get("priority", 4)) - 1
                    self.log(WARNING_STREAM_INDEX, log)
                elif log["priority"] <= 3:
                    self.log(WARNING_STREAM_INDEX, log)

    ##########################################################################################################
    # Diagnostics utility functions
    ##########################################################################################################
    def get_health(self, check_memory_usage: bool = True) -> dict[str, Any]:
        """Get the health of the device.

        check_memory_usage Set False to prevent recursion when used for diagnostics collection.
        """
        health: dict[str, Any] = {}
        try:
            cpu_temp: str = ""
            bytes_written = 0
            latest_bytes_written = 0
            bytes_sent = 0
            latest_bytes_sent = 0
            sc_mount_size = ""
            get_throttled_output = ""
            process_list_str = ""
            ssid = ""
            signal_strength = ""
            if root_cfg.running_on_rpi:
                cpu_temp = str(psutil.sensors_temperatures()["cpu_thermal"][0].current)  # type: ignore

                # Get the connected SSID
                ssid, signal_strength = DeviceHealth.get_wifi_ssid_and_signal()

                # We need to call the "vcgencmd get_throttled" command to get the current throttled state
                # Output is "throttled=0x0"
                get_throttled_output = utils.run_cmd(
                    "sudo vcgencmd get_throttled", ignore_errors=True, grep_strs=["throttled"]
                )
                get_throttled_output = get_throttled_output.replace("throttled=", "")

                # Get the number of disk writes
                sdiskio = psutil.disk_io_counters()
                if sdiskio is not None:
                    latest_bytes_written = sdiskio.write_bytes
                bytes_written = max(latest_bytes_written - self.cum_bytes_written, 0)
                self.cum_bytes_written = latest_bytes_written

                # Get the latest number of bytes sent
                netio = psutil.net_io_counters()
                if netio is not None:
                    latest_bytes_sent = netio.bytes_sent
                bytes_sent = max(latest_bytes_sent - self.cum_bytes_sent, 0)
                self.cum_bytes_sent = latest_bytes_sent

                # Get the size of the /rpi_core mount
                usage = psutil.disk_usage(str(root_cfg.ROOT_WORKING_DIR))
                sc_mount_size = f"{usage.total / (1024**3):.2f} GB"

                # Running processes
                # Drop any starting / or . characters
                # And convert the process list to a simple comma-seperated string with no {} or ' or "
                # characters
                if root_cfg.system_cfg:
                    process_set = utils.check_running_processes(
                        search_string=f"{root_cfg.system_cfg.my_start_script}"
                    ).union(utils.check_running_processes(search_string="python "))
                    process_list_str = str(process_set).replace("{", "").replace("}", "")
                    process_list_str = process_list_str.replace("'", "").replace('"', "").strip()
                else:
                    process_list_str = ""

            # Check update status by getting the last modified time of the rpi_installer_ran file
            # This file is created when the rpi_installer.sh script is run
            # and is used to track the last time the system was updated
            last_update_time: str = ""
            rpi_installer_file = root_cfg.FLAGS_DIR / "rpi_installer_ran"
            if os.path.exists(rpi_installer_file):
                last_update_time = api.utc_to_iso_str(os.path.getmtime(rpi_installer_file))

            # Get the IP address of the wlan0 interface
            if root_cfg.running_on_rpi:
                target_interface = "wlan0"
            else:
                target_interface = "WiFi"
            ip_address: str = ""
            snicaddr = psutil.net_if_addrs().get(target_interface, [])
            if snicaddr:
                ip_addresses = [addr.address for addr in snicaddr if addr.family == socket.AF_INET]
                if ip_addresses:
                    ip_address = str(ip_addresses[0])

            # Packet loss - this is the number of ping failures divided by the number of pings sent
            if self.device_manager is None:
                packet_loss: float = 0.0
                ping_failure_count_run: int = 0
            else:
                ping_failure_count_run = self.device_manager.ping_failure_count_run
                fail_count = max(
                    self.device_manager.ping_failure_count_all - self.last_ping_failure_count_all, 0
                )
                success_count = max(
                    self.device_manager.ping_success_count_all - self.last_ping_success_count_all, 0
                )
                packet_loss = fail_count / (fail_count + success_count + 1)
                # Reset our local counts
                self.last_ping_failure_count_all = self.device_manager.ping_failure_count_all
                self.last_ping_success_count_all = self.device_manager.ping_success_count_all

            # Total memory
            total_memory = psutil.virtual_memory().total
            total_memory_gb = round(total_memory / (1024**3), 2)

            # Memory usage - if greater than 75% then generate some diagnostics
            memory_usage = psutil.virtual_memory().percent
            if check_memory_usage and memory_usage > 75:
                if root_cfg.running_on_rpi:
                    DeviceHealth.log_top_memory_processes()
                    # Running low on free RAM can cause any OS process to be killed to free up memory, and can
                    # cause performance degradation. Triggering a controlled reboot at 90% memory usage is
                    # generally considered good practice to recover before performance degrades.
                    if memory_usage > 90:
                        logger.error(root_cfg.RAISE_WARN() + "Memory usage >90%, rebooting")
                        DiagnosticsBundle.collect("Memory usage >90%, rebooting")
                        utils.run_cmd("sudo reboot", ignore_errors=True)

            # Get the expidite version and user code version from the files
            # Stored in .expidite/user_code_version and .expidite/expidite_code_version
            expidite_version, user_code_version = root_cfg.get_version_info()

            health = {
                "boot_time": api.utc_to_iso_str(psutil.boot_time()),
                "last_update_time": str(last_update_time),
                # Returns the percentage of CPU usage since the last call to this function
                "cpu_percent": str(psutil.cpu_percent(0)),
                "total_memory_gb": str(total_memory_gb),
                "memory_percent": str(memory_usage),
                "memory_free": str(int(psutil.virtual_memory().free / 1000000)) + "M",
                "disk_percent": str(psutil.disk_usage("/").percent),
                "disk_bytes_written_in_period": str(bytes_written),
                "io_bytes_sent": str(bytes_sent),
                "expidite_mount_size": str(sc_mount_size),
                "expidite_mount_percent": str(psutil.disk_usage(str(root_cfg.ROOT_WORKING_DIR)).percent),
                "packet_loss": str(packet_loss),
                "current_ping_fail_run": str(ping_failure_count_run),
                "cpu_temperature": str(cpu_temp),  # type: ignore
                "ssid": ssid,
                "signal_strength": signal_strength,
                "ip_address": str(ip_address),
                "power_status": str(get_throttled_output),
                "process_list": process_list_str,
                "expidite_version": expidite_version,
                "user_code_version": user_code_version,
            }

        except Exception as e:
            logger.error(root_cfg.RAISE_WARN() + "Failed to get telemetry: " + str(e), exc_info=True)

        return health

    # Function to get diagnostics on the top 3 memory-using processes
    @staticmethod
    def log_top_memory_processes(num_processes: int = 5) -> None:
        # Create a list of all processes with their memory usage
        # It's possible for processes to disappear between the time we get the list and the time we log it
        # so we need to be careful about this
        processes = []
        for proc in psutil.process_iter(attrs=["pid", "name", "memory_info", "cmdline"]):
            # The memory_info is in a pmem object, so we need to extract the rss value
            rss = proc.info["memory_info"].rss
            processes.append((rss, proc.info))

        # Sort the list of processes by memory usage (rss) in descending order
        all_processes = sorted(processes, key=lambda x: x[0], reverse=True)
        top_processes = all_processes[:num_processes]

        # Format the information for the top processes
        log_string = f"Memory at {psutil.virtual_memory().percent}%; top processes: "
        for _, info in top_processes:
            # Combine the command line arguments into a single string, but drop any words starting with "-"
            if root_cfg.running_on_rpi:
                cmd_line = " ".join([arg for arg in info["cmdline"] if not arg.startswith("-")])
            else:
                cmd_line = info["name"]
            log_string += f"[{cmd_line}]({info['pid']})={info['memory_info'].rss / (1024**2):.2f}MB, "
        logger.warning(log_string)

    @staticmethod
    def get_wifi_ssid_and_signal() -> tuple[str, str]:
        """
        Get the SSID of the wlan0 interface.

        Returns:
            The SSID as a string, or "Not connected" if no SSID is found.
        """
        if root_cfg.running_on_rpi:
            return DeviceHealth.get_wifi_ssid_and_signal_on_rpi()

        if root_cfg.running_on_windows:
            return DeviceHealth.get_wifi_ssid_and_signal_on_windows()

        return ("Unsupported platform", "-1")

    @staticmethod
    def get_wifi_ssid_and_signal_on_rpi() -> tuple[str, str]:
        try:
            output = utils.run_cmd(
                cmd="nmcli -g SSID,IN-USE,SIGNAL device wifi | grep '*'", ignore_errors=True
            )
            # The return output contains a string like "SSID:*:95".  We need to strip out the ":*"
            # and return just the SSID and the signal strength
            if output:
                parts = output.split(":")
                if len(parts) >= 3:
                    ssid = parts[0]
                    signal_strength = str(int(parts[2]))
                    return (ssid, signal_strength)
                logger.warning(f"Unexpected nmcli output format: {output}")
                return ("Not connected", "0")
            logger.warning("No nmcli output")
            return ("Not connected", "0")
        except Exception as e:
            logger.warning(f"Failed to get SSID: {e}")
            return ("Not connected", "-1")

    @staticmethod
    def get_wifi_ssid_and_signal_on_windows() -> tuple[str, str]:
        try:
            output = subprocess.check_output(["netsh", "wlan", "show", "interfaces"], universal_newlines=True)
            for line in output.split("\n"):
                if "SSID" in line and "BSSID" not in line:
                    return (line.split(":")[1].strip(), "-1")
            return ("Not connected", "0")
        except subprocess.CalledProcessError:
            return ("Not connected", "-1")

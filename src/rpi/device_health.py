import os
import socket
import subprocess
from datetime import datetime
from typing import Any, Optional

import psutil

from sensor_core import api
from sensor_core import configuration as root_cfg
from sensor_core.dp_config_objects import SensorCfg, Stream
from sensor_core.sensor import Sensor
from sensor_core.utils import utils

if root_cfg.running_on_rpi:
    from systemd import journal  # type: ignore
    def get_logs(since: Optional[datetime] = None, 
                 min_priority: Optional[int] = None,
                 grep_str: Optional[str] = None,
                 max_logs: int = 1000) -> list[dict[str, Any]]:
        """
        Fetch logs from the system journal.

        Args:
            since (datetime): A timestamp to fetch logs since.
            min_priority (int): The priority level (e.g., 6 for informational, 4 for warnings).
            grep_str (str): A string to filter logs by message content.
            max_logs (int): Maximum number of logs to fetch.

        Returns:
            list[dict[str, Any]]: A list of log entries.
        """
        logs:list[dict] = []
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
        for i, entry in enumerate(reader):
            priority = int(entry.get("PRIORITY", 0))
            if (((api.RAISE_WARN_TAG in entry.get("MESSAGE", "")) or
                 (min_priority is None or priority <= min_priority)) and
                (grep_str is None or grep_str in entry.get("MESSAGE", ""))):
                if i >= max_logs:
                    break
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
        logger.info(f"Fetched {len(logs)} logs from the journal.")

        return logs

logger = root_cfg.setup_logger("sensor_core")

# HEART - special datastream for recording device & system health
HEART_FIELDS = [
    "boot_time",
    "last_update_timestamp",
    "cpu_percent",
    "total_memory_gb",
    "memory_percent",
    "memory_free",
    "disk_percent",
    "disk_bytes_written_in_period",
    "io_bytes_sent",
    "sc_mount_size",
    "sc_ram_percent",
    "cpu_temperature",
    "ssid",
    "ip_address",
    "power_status",
    "process_list",
    "sensor_core_version",
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
        Stream("Health heartbeat stream", 
               api.HEART_DS_TYPE_ID, 
               HEART_STREAM_INDEX, 
               format=api.FORMAT.LOG, 
               fields=HEART_FIELDS,
               cloud_container=root_cfg.my_device.cc_for_system_records),
        Stream("Warning log stream", 
               api.WARNING_DS_TYPE_ID, 
               WARNING_STREAM_INDEX, 
               format=api.FORMAT.LOG, 
               fields=WARNING_FIELDS,
               cloud_container=root_cfg.my_device.cc_for_system_records),
    ],
)

class DeviceHealth(Sensor):
    """Monitors device health and provides data as a SensorCore datastream.
    Produces the following data:
    - HEART (DS type ID) provides periodic heartbeats with device health data up to cloud storage.
    - WARNINGS (DS type ID) captures warning and error logs produced by any component, aggregates
      them, and sends them up to cloud storage.
    """

    def __init__(self) -> None:
        super().__init__(DEVICE_HEALTH_CFG)
        ###############################
        # Telemetry tracking
        ###############################
        self.last_ran = api.utc_now()
        self.device_id = root_cfg.my_device_id
        self.cum_bytes_written = 0
        self.cum_bytes_sent = 0
        self.log_counter = 0
        
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
        We capture logs tagged with the RAISE_WARN_TAG and all logs with priority <=4 (Warning)."""

        if root_cfg.running_on_rpi:
            since_time = self.last_ran
            self.last_ran = api.utc_now()
            logs = get_logs(since=since_time, min_priority=6)

            for log in logs:
                if api.RAISE_WARN_TAG in log:
                    self.log(WARNING_STREAM_INDEX, log)
                elif log["priority"] <= 4:
                    self.log(WARNING_STREAM_INDEX, log)
            

    ############################################################################################################
    # Diagnostics utility functions
    ############################################################################################################
    def get_health(self) -> dict[str, Any]:
        """Get the health of the device."""
        health: dict[str, Any] = {}
        try:
            cpu_temp: str = ""
            bytes_written = 0
            bytes_sent = 0
            sc_mount_size = ""
            get_throttled_output = ""
            process_list_str = ""
            ssid = ""
            if root_cfg.running_on_rpi:
                cpu_temp = str(psutil.sensors_temperatures()["cpu_thermal"][0].current) # type: ignore

                # Get the connected SSID
                ssid = DeviceHealth.get_wifi_ssid()

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

                # Get the size of the /sensor_core mount
                # Parse the output to get the size of the mount (equivalent to "awk 'NR==2{print $2}'")
                usage = psutil.disk_usage("/sensor_core")
                sc_mount_size = f"{usage.total / (1024**3):.2f} GB"

                # Running processes
                # for each process in the list, strip any text before "sensor_core" or "dua"
                # Drop any starting / or . characters
                # And convert the process list to a simple comma-seperated string with no {} or ' or " 
                # characters          
                if root_cfg.system_cfg:      
                    process_set = (
                        utils.check_running_processes(
                            search_string=f"{root_cfg.system_cfg.my_start_script}").union(
                                utils.check_running_processes(search_string="python "))
                    )
                    process_list_str = str(process_set).replace("{", "").replace("}", "")
                    process_list_str = process_list_str.replace("'", "").replace('"', "").strip()
                else:
                    process_list_str = ""

            # Check update status by getting the last modified time of the rpi_installer_ran file
            # This file is created when the rpi_installer.sh script is run
            # and is used to track the last time the system was updated
            last_update_timestamp: str = ""
            rpi_installer_file = root_cfg.FLAGS_DIR / "rpi_installer_ran"
            if os.path.exists(rpi_installer_file):
                last_update_timestamp = api.utc_to_iso_str(os.path.getmtime(rpi_installer_file))

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

            # Grab the code version of the current SensorCore code
            try:
                from sensor_core import __version__
                sensor_core_version = __version__
            except ImportError:
                logger.warning(f"{root_cfg.RAISE_WARN()}Failed to get SensorCore version, using unknown")
                sensor_core_version = "unknown"

            # Total memory
            total_memory = psutil.virtual_memory().total
            total_memory_gb = round(total_memory / (1024**3), 2)

            # Memory usage - if greater than 60% then generate some diagnostics
            memory_usage = psutil.virtual_memory().percent
            if memory_usage > 75:
                if root_cfg.running_on_rpi:
                    DeviceHealth.log_top_memory_processes()
                    if memory_usage > 95:
                        logger.error(root_cfg.RAISE_WARN() + "Memory usage >95%, rebooting")
                        utils.run_cmd("sudo reboot", ignore_errors=True)

            health = {
                "boot_time": api.utc_to_iso_str(psutil.boot_time()),
                "last_update_timestamp": str(last_update_timestamp),
                # Returns the percentage of CPU usage since the last call to this function
                "cpu_percent": str(psutil.cpu_percent(0)),
                "total_memory_gb": str(total_memory_gb),
                "memory_percent": str(memory_usage),
                "memory_free": str(int(psutil.virtual_memory().free / 1000000)) + "M",
                "disk_percent": str(psutil.disk_usage("/").percent),
                "disk_bytes_written_in_period": str(bytes_written),
                "io_bytes_sent": str(bytes_sent),
                "sc_mount_size": str(sc_mount_size),
                "sc_ram_percent": str(
                    psutil.disk_usage(str(root_cfg.ROOT_WORKING_DIR)).percent
                ),  # Need to parse the output of sensors_temperatures() to get the current CPU temperature
                # Output is {'cpu_thermal': [shwtemp(label='', current=46.251, high=110.0, critical=110.0)]}
                "cpu_temperature": str(cpu_temp),  # type: ignore
                "ssid": ssid,
                "ip_address": str(ip_address),
                "power_status": str(get_throttled_output),
                "process_list": process_list_str,
                "sensor_core_version": str(sensor_core_version),
            }

        except Exception as e:
            logger.error(root_cfg.RAISE_WARN() + "Failed to get telemetry: " + str(e), exc_info=True)

        return health

    # Function to get diagnostics on the top 3 memory-using processes
    @staticmethod
    def log_top_memory_processes(num_processes: int=5) -> None:
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
        for rss, info in top_processes:
            # Combine the command line arguments into a single string, but drop any words starting with "-"
            if root_cfg.running_on_rpi:
                cmd_line = " ".join([arg for arg in info["cmdline"] if not arg.startswith("-")])
            else:
                cmd_line = info["name"]
            log_string += f"[{cmd_line}]({info['pid']})={info['memory_info'].rss / (1024**2):.2f}MB, "
        logger.warning(log_string)

    @staticmethod
    def get_wifi_ssid() -> str:
        """
        Get the SSID of the wlan0 interface using the `iw` command.

        Returns:
            The SSID as a string, or "Not connected" if no SSID is found.
        """
        if root_cfg.running_on_rpi:
            try:
                output = utils.run_cmd(cmd="bash -c 'iw dev wlan0 link'", ignore_errors=True).strip()
                logger.debug(f"iw output: {output}")
                for line in output.split("\n"):
                    if "SSID:" in line:
                        logger.debug(f"Found SSID line: {line}")
                        return line.split("SSID:")[1].strip()
                return "Not connected"
            except Exception as e:
                logger.info(f"Failed to get SSID: {e}")
                return "Not connected"
        elif root_cfg.running_on_windows:
            try:
                output = subprocess.check_output(["netsh", "wlan", "show", "interfaces"], 
                                                 universal_newlines=True)
                for line in output.split("\n"):
                    if "SSID" in line and "BSSID" not in line:
                        return line.split(":")[1].strip()
                return "Not connected"
            except subprocess.CalledProcessError:
                return "Not connected"
        else:
            return "Unsupported platform"
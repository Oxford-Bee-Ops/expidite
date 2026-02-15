##############################################################################################################
# Description: This script is used to run the bcli command.
##############################################################################################################
import os
import queue
import subprocess
import sys
import threading
import time
from datetime import timedelta
from pathlib import Path

import click
from crontab import CronTab

from expidite_rpi.core import api, device_health
from expidite_rpi.core import configuration as root_cfg
from expidite_rpi.core.cloud_connector import AsyncCloudConnector, CloudConnector
from expidite_rpi.core.edge_orchestrator import EdgeOrchestrator
from expidite_rpi.rpi_core import RpiCore
from expidite_rpi.utils import utils
from expidite_rpi.utils.utils_clean import disable_console_logging

logger = root_cfg.setup_logger("expidite")

dash_line = "########################################################"
header = dash_line + "\n\n"

##############################################################################################################
# Utility functions
##############################################################################################################


# Wrapper for utils.run_cmd so that we can display error rather than throwing an exception
def run_cmd(cmd: str) -> str:
    """Run a command and return its output or an error message."""
    if not root_cfg.running_on_rpi:
        return "This command only works on a Raspberry Pi"
    try:
        return utils.run_cmd(cmd, ignore_errors=True)
    except Exception as e:
        return f"Error: {e}"


def reader(proc: subprocess.Popen, queue: queue.Queue) -> None:
    """Read 'stdout' from the subprocess and put it into the queue.

    Args:
        proc: The subprocess to read from.
        queue: The queue to store the output lines.
    """
    if proc.stdout:
        for line in iter(proc.stdout.readline, b""):
            queue.put(line)


def run_cmd_live_echo(cmd: str) -> str:
    """Run a command and echo its output in real-time.

    Args:
        cmd: The command to run.

    Returns:
        A string indicating success or an error message.
    """
    if not root_cfg.running_on_rpi:
        return "This command only works on a Raspberry Pi"
    try:
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
        q: queue.Queue = queue.Queue()
        reader_thread = threading.Thread(target=reader, args=(process, q))
        reader_thread.start()

        while True:
            try:
                line = q.get(timeout=2)
                click.echo(line.decode("utf-8").strip())
            except queue.Empty:
                if process.poll() is not None:
                    break
    except Exception as e:
        return f"Error: {e}"
    finally:
        if process and process.poll() is None:
            process.terminate()  # Ensure the process is terminated

    return "Command executed successfully."


def check_if_setup_required() -> None:
    """Check if setup is required by verifying keys and device inventory."""
    attempts = 0
    max_attempts = 3
    while not check_keys_env():
        attempts += 1
        if attempts >= max_attempts:
            click.echo("Setup not completed. Exiting...")
            sys.exit(1)
        click.echo("Press any key to retry setup...")
        click.getchar()

    # Check if device is found in inventory
    check_device_in_inventory()


def check_keys_env() -> bool:
    """Check if the keys.env exists in ./rpi_core and is not empty.

    Returns:
        True if the keys.env file exists and is valid, False otherwise.
    """
    success, error = root_cfg.check_keys()
    if success:
        return True

    # Help the user setup keys
    click.echo(f"{dash_line}")
    click.echo(f"# {error}")
    click.echo("# ")
    click.echo(f"# Create a file called {root_cfg.KEYS_FILE} in {root_cfg.CFG_DIR}.")
    click.echo("# Add a key called 'cloud_storage_key'.")
    click.echo("# The value should be the Shared Access Signature for your Azure storage account.")
    click.echo("# You'll find this in portal.azure.com > Storage accounts > Security + networking.")
    click.echo("# ")
    click.echo("# The final line will look like:")
    click.echo(
        '# cloud_storage_key="DefaultEndpointsProtocol=https;AccountName=mystorageprod;'
        "AccountKey=UnZzSivXKjXl0NffCODRGqNDFGCwSBHDG1UcaIeGOdzo2zfFs45GXTB9JjFfD/"
        'ZDuaLH8m3tf6+ASt2HoD+w==;EndpointSuffix=core.windows.net;"'
    )
    click.echo("# ")
    click.echo("# Press any key to continue once you have done so")
    click.echo("# ")
    click.echo(f"{dash_line}")
    return False


def check_device_in_inventory() -> None:
    """Check if this device's ID is found in the fleet configuration inventory."""
    try:
        # Check if the current device is in the already-loaded inventory
        if root_cfg.my_device_id not in root_cfg.INVENTORY:
            click.echo(f"{dash_line}")
            click.echo("# DEVICE NOT FOUND IN INVENTORY")
            click.echo("# ")
            click.echo(
                f"# This device ID ({root_cfg.my_device_id}) is not configured in your fleet inventory."
            )
            click.echo(
                f"# Fleet inventory: "
                f"{root_cfg.system_cfg.my_fleet_config if root_cfg.system_cfg else 'NOT SET'}"
            )
            click.echo("# ")
            click.echo("# This typically means one of:")
            click.echo("# 1. The device's MAC address has not been added to your fleet configuration")
            click.echo("# 2. The fleet configuration module is not accessible or has errors")
            click.echo("# 3. The system.cfg file points to an incorrect fleet configuration")
            click.echo("# ")
            click.echo("# To fix this:")
            click.echo("# 1. Get this device's MAC address: cat /sys/class/net/wlan0/address")
            click.echo(f"#    (This device's MAC-based ID: {root_cfg.my_device_id})")
            click.echo("# 2. Add the device configuration to your fleet config file")
            click.echo("# 3. Update your git repository with the new configuration")
            click.echo("# 4. Run 'Update Software' from the Maintenance menu")
            click.echo("# ")
            click.echo("# You can continue to use the CLI for maintenance and debugging,")
            click.echo("# but RpiCore will not start properly until this device is configured.")
            click.echo("# ")
            click.echo(f"{dash_line}")
    except Exception as e:
        logger.debug(f"Error checking device inventory: {e}")
        # Don't show this error to user as it may be normal during setup


class InteractiveMenu:
    """Interactive menu for navigating commands."""

    def __init__(self) -> None:
        self.sc = RpiCore()
        inventory = root_cfg.load_configuration()
        logger.debug(f"Inventory: {inventory}")
        if inventory:
            self.sc.configure(inventory)

    ##########################################################################################################
    # Main menu functions
    ##########################################################################################################
    def view_status(self) -> None:
        """View the current status of the device."""
        try:
            click.echo(f"{dash_line}\n")
            click.echo(self.sc.status(verbose=False))
            self.display_score_logs()
            self.display_sensor_logs()
        except Exception as e:
            click.echo(f"Error in script start up: {e}")

    def view_rpi_core_config(self) -> None:
        """View the rpi core configuration."""
        # Check we have blob storage access
        if not check_keys_env():
            return

        click.echo(f"\n{dash_line}")
        click.echo("# Sensors & datastreams")
        click.echo(f"{dash_line}\n")
        edge_orch = EdgeOrchestrator.get_instance()
        if edge_orch is not None:
            edge_orch.load_config()
            for i, dptree in enumerate(edge_orch.dp_trees):
                sensor_cfg = dptree.sensor.config
                click.echo(
                    f"{i}> {sensor_cfg.sensor_type} {sensor_cfg.sensor_index}  {sensor_cfg.sensor_model}"
                )
                streams = dptree.sensor.config.outputs
                if streams is not None:
                    for stream in streams:
                        click.echo(f"  {stream.type_id}: - {stream.description}")

        # Display system.cfg
        if root_cfg.system_cfg:
            click.echo(f"\n{dash_line}")
            click.echo("# SYSTEM CONFIGURATION")
            click.echo(f"{dash_line}")
            expidite_version, user_code_version = root_cfg.get_version_info()
            click.echo(f"Expidite version: {expidite_version}")
            click.echo(f"User code version: {user_code_version}")
            # Display each field in the root_cfg.system_cfg BaseSettings object
            # Convert the base settings to a dictionary
            system_cfg_dict = root_cfg.system_cfg.model_dump()
            for field in system_cfg_dict:
                value = system_cfg_dict[field]
                click.echo(f"{field}: {value}")

        click.echo(f"\n{dash_line}")
        click.echo("# EXPIDITE CONFIGURATION")
        click.echo(f"{dash_line}")
        click.echo(f"{self.sc.display_configuration()}")

    ##########################################################################################################
    # Sensing menu functions
    ##########################################################################################################
    def trigger_sensing(self) -> None:
        """Trigger sensing on all sensors by setting the sensor flag."""
        # Need to ask for input on duration of recording in seconds
        click.echo(f"{dash_line}")
        click.echo("# TRIGGER SENSING ON ALL SENSORS")
        click.echo("Note: this will have no effect if sensors are already recording continuously.\n")
        duration = click.prompt("Enter duration of recording in whole seconds", type=int, default=60)
        click.echo(f"Triggering sensing on all sensors for {duration} seconds...")
        # Write the duration to a file or set a flag that sensors can read
        with open(root_cfg.SENSOR_TRIGGER_FLAG, "w") as f:
            f.write(str(duration))
        # Provide a count down timer
        for i in range(duration, 0, -1):
            click.echo(f"Sensing in progress... {i} seconds remaining", nl=False)
            click.echo("\r", nl=False)
            time.sleep(1)
        # When the countdown is complete, remove the trigger file
        if root_cfg.SENSOR_TRIGGER_FLAG.exists():
            root_cfg.SENSOR_TRIGGER_FLAG.unlink()
        click.echo("Sensing complete.")

    ##########################################################################################################
    # Debug menu functions
    ##########################################################################################################
    def journalctl(self) -> None:
        """Continuously display journal logs in real time."""
        # Ask if the user wants to specify a grep filter
        click.echo("Do you want to filter the logs? (y/n)")
        char = click.getchar()
        click.echo(char)
        if char == "y":
            click.echo("Enter the grep filter string:")
            filter_str = input()
        else:
            filter_str = ""
        click.echo("Press Ctrl+C to exit...\n")
        if root_cfg.running_on_windows:
            click.echo("This command only works on Linux. Exiting...")
            return
        try:
            if filter_str != "":
                process = subprocess.Popen(
                    ["journalctl", "-f", "|", "grep -i", filter_str],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
            else:
                process = subprocess.Popen(
                    ["journalctl", "-f"], stdout=subprocess.PIPE, stderr=subprocess.PIPE
                )
            while True:
                if process.stdout is not None:
                    line = process.stdout.readline().decode("utf-8").strip()
                    # Filter out the dull spam
                    if "pam_unix" in line:
                        line = ""
                    if line != "":
                        click.echo(line)
                        sys.stdout.flush()  # Flush the output to ensure real-time display
                    time.sleep(0.1)  # Adjust the refresh interval as needed
        except KeyboardInterrupt:
            click.echo("\nExiting...")

    @staticmethod
    def display_logs(logs: list[dict]) -> None:
        for log in logs:
            # Nicely format the log by printing the timestamp and message
            log["timestamp"] = api.utc_to_iso_str(log["time_logged"])
            click.echo(f"\n{log['timestamp']} - {log['priority']} - {log['message']}")

    def display_errors(self) -> None:
        """Display error logs."""
        if root_cfg.running_on_windows:
            click.echo("This command only works on Linux. Exiting...")
            return
        click.echo("\n")
        click.echo(f"{dash_line}")
        click.echo("# ERROR LOGS")
        click.echo("# Displaying error logs from the last 4 hours")
        click.echo(f"{dash_line}")
        since_time = api.utc_now() - timedelta(hours=4)
        logs = device_health.get_logs(since=since_time, min_priority=4)
        self.display_logs(logs)
        # Cross check with simple journalctl command
        click.echo("\n")
        click.echo(f"{dash_line}")
        click.echo("# ERROR LOGS (journalctl grep check)")
        click.echo(f"{dash_line}")
        utils.run_cmd("journalctl", ignore_errors=True, grep_strs=["error"])

    def display_rpi_core_logs(self) -> None:
        """Display regular rpi_core logs."""
        if root_cfg.running_on_windows:
            click.echo("This command only works on Linux. Exiting...")
            return
        click.echo(f"{dash_line}")
        click.echo("# Expidite logs")
        click.echo("# Displaying expidite logs for the last 15 minutes")
        click.echo(f"{dash_line}")
        since_time = api.utc_now() - timedelta(minutes=15)
        logs = device_health.get_logs(since=since_time, min_priority=6, grep_str=["expidite"])
        self.display_logs(logs)

    def display_sensor_logs(self) -> None:
        if root_cfg.running_on_windows:
            click.echo("This command only works on Linux. Exiting...")
            return
        click.echo(f"{dash_line}")
        click.echo("# Sensor logs")
        click.echo("# Displaying sensor output logs (last 30 minutes)")
        click.echo(f"{dash_line}")
        since_time = api.utc_now() - timedelta(minutes=30)
        logs = device_health.get_logs(
            since=since_time, min_priority=6, grep_str=[api.TELEM_TAG, "Save log: "]
        )
        log_dict_str = ""
        try:
            for log in logs:
                # Nicely format sensor logs
                # The message contains a dictionary after "Save log: " which is enclosed in {}
                # We want to extract that dictionary and display it as key: value pairs
                log_dict_str = log["message"].split("Save log: ")[1]
                log_dict: dict = eval(log_dict_str)  # Convert string to dictionary
                data_type_id = log_dict.pop("data_type_id")
                timestamp = log_dict.pop("timestamp", "UNKNOWN")

                # Don't display SCORE & SCORP logs as they're spammy
                if data_type_id in [api.SCORE_DS_TYPE_ID, api.SCORP_DS_TYPE_ID]:
                    continue
                # Remove "noisy" keys that don't add value
                for k in ["device_id", "version_id", "timestamp", "device_name"]:
                    log_dict.pop(k, "")
                log["message"] = ", ".join([f"{k}={v!s}" for k, v in log_dict.items()])
                click.echo(f"\n{data_type_id} >>> {timestamp} >>> {log['message']}")
            if not logs:
                click.echo("No sensor output logs found.")
            click.echo(f"\n{dash_line}\n")
        except Exception:
            logger.exception(f"Error parsing log dictionary {log_dict_str}")

    def display_score_logs(self) -> None:
        """View the SCORE logs."""
        if root_cfg.running_on_windows:
            click.echo("This command only works on Linux. Exiting...")
            return
        click.echo(f"\n{dash_line}")
        click.echo("# Expidite SCORE logs of sensor output (last 15 minutes)")
        click.echo(f"{dash_line}")
        since_time = api.utc_now() - timedelta(minutes=15)
        logs = device_health.get_logs(
            since=since_time, min_priority=6, grep_str=[api.TELEM_TAG, "Save log: ", "SCORE"]
        )

        try:
            for log in logs:
                # Nicely format SCORE logs
                log_dict_str = log["message"].split("Save log: ")[1]
                log_dict: dict = eval(log_dict_str)
                observed_type_id = log_dict.get("observed_type_id", "UNKNOWN")
                observed_sensor_index = log_dict.get("observed_sensor_index", "UNKNOWN")
                count = log_dict.get("count", "UNKNOWN")
                sample_period = log_dict.get("sample_period", "UNKNOWN")
                click.echo(
                    f"\n{observed_type_id + ' ' + observed_sensor_index:<20} | "
                    f"{count!s:<4} | {sample_period:<20}"
                )
            if not logs:
                click.echo("No SCORE logs found.")
            click.echo(f"\n{dash_line}\n")
        except Exception:
            logger.exception("Error parsing log dictionary")

    def display_running_processes(self) -> None:
        # Running processes
        # Drop any starting / or . characters
        # And convert the process list to a simple comma-seperated string with no {} or ' or "
        # characters
        if not root_cfg.system_cfg.is_valid:
            click.echo("System.cfg is not set. Please check your installation.")
            return
        process_set = utils.check_running_processes(search_string=f"{root_cfg.system_cfg.my_start_script}")
        process_list_str = (
            str(process_set).replace("{", "").replace("}", "").replace("'", "").replace('"', "").strip()
        )
        click.echo(f"{dash_line}")
        click.echo("# Display running RpiCore processes")
        click.echo(f"{dash_line}\n")
        click.echo(f"{process_list_str}\n")

    def show_recordings(self) -> None:
        # List all files under the root_working_dir
        click.echo(f"{dash_line}")
        click.echo("# RpiCore recordings")
        click.echo(f"{dash_line}")
        click.echo("Recording files:")
        click.echo(run_cmd(f"ls -lhR {root_cfg.ROOT_WORKING_DIR}*"))
        click.echo("\n")

    ##########################################################################################################
    # Maintenance menu functions
    ##########################################################################################################
    def update_software(self) -> None:
        """Update the software to the latest version."""
        click.echo("Running update to get latest code...")
        if root_cfg.running_on_windows:
            click.echo("This command only works on Linux. Exiting...")
            return
        # Check if the scripts directory exists
        if not root_cfg.system_cfg.is_valid:
            click.echo("System.cfg is not set. Please check your installation.")
            return
        scripts_dir = Path.home() / root_cfg.system_cfg.venv_dir / "scripts"
        if scripts_dir.exists():
            run_cmd_live_echo(f"sudo -u $USER {scripts_dir}/rpi_installer.sh")
        else:
            click.echo(
                f"Error: scripts directory does not exist at {scripts_dir}. Please check your installation."
            )

    def start_rpi_core(self) -> None:
        """Start the RpiCore service."""
        click.echo("Starting RpiCore...")

        # If my_start_script is a resolvable module in this environment, then we use that to start the service
        # using that user-provided script.
        if (
            root_cfg.system_cfg is None
            or root_cfg.system_cfg.my_start_script is None
            or root_cfg.system_cfg.my_start_script == root_cfg.FAILED_TO_LOAD
        ):
            click.echo("System.cfg has no my_start_script configuration")
            click.echo("Do you want to start RpiCore using the default configuration? (y/n)")
            char = click.getchar()
            click.echo(char)
            if char != "y":
                click.echo("Exiting...")
                return
            click.echo("Starting RpiCore using default configuration...")
            self.sc.start()
        else:
            try:
                my_start_script = root_cfg.system_cfg.my_start_script
                # Try creating an instance and calling main()
                # This will raise an ImportError if the module is not found or if the main() function is not
                # defined in the module
                module = __import__(my_start_script, fromlist=["main"])
                main_func = getattr(module, "main", None)
                if main_func is None:
                    click.echo(f"main() function not found in {my_start_script}")
                    click.echo("Exiting...")
                    return
            except ImportError as e:
                logger.exception(f"{root_cfg.RAISE_WARN()}Module {my_start_script} not resolvable")
                click.echo(f"Module {my_start_script} not resolvable ({e})")
                click.echo("Exiting...")
                return
            else:
                click.echo(f"Found {my_start_script}. Starting RpiCore...")
                if root_cfg.running_on_windows:
                    click.echo("This command only works on Linux. Exiting...")
                    return
                # Check whether the script is already running
                if utils.is_already_running(my_start_script):
                    click.echo(f"{my_start_script} is already running.")
                    return
                cmd = (
                    f"bash -c 'source {root_cfg.HOME_DIR}/{root_cfg.system_cfg.venv_dir}/bin/activate && "
                    f"nohup python -m {my_start_script} 2>&1 | /usr/bin/logger -t EXPIDITE &'"
                )
                click.echo(f"Running command: {cmd}")
                run_cmd_live_echo(cmd)

        click.echo("RpiCore started.")

    def stop_rpi_core(self, pkill: bool) -> None:
        """Stop the RpiCore service."""
        click.echo("Stopping RpiCore... this may take up to 180s to complete.")
        # We just need to "touch" the stop file to stop the service
        root_cfg.STOP_EXPIDITE_FLAG.touch()

        if pkill and root_cfg.system_cfg:
            run_cmd(f"sudo pkill -f 'python -m {root_cfg.system_cfg.my_start_script}'")

    def enable_rpi_connect(self) -> None:
        """Enable the RPi Connect service."""
        click.echo("Enabling RPi Connect service...")
        if not root_cfg.running_on_rpi:
            click.echo("This command only works on a Raspberry Pi")
            return
        click.echo("Copy the URL returned by this command to a browser ")
        click.echo("and authenticate the request to your Raspberry Pi connect account.")
        run_cmd_live_echo("rpi-connect on")
        run_cmd_live_echo("rpi-connect signin")
        click.echo("\nHit any key to continue once you've signed in.")
        click.getchar()
        run_cmd_live_echo("rpi-connect on")
        run_cmd("loginctl enable-linger")
        click.echo("RPi Connect service enabled.")

    def enter_review_mode(self) -> None:
        """Enter review mode by setting the current timestamp in the review mode flag file."""
        # Write the current timestamp to the review mode flag file
        root_cfg.REVIEW_MODE_FLAG.write_text(str(int(time.time())))

    def exit_review_mode(self) -> None:
        """Exit review mode by deleting the review mode flag file."""
        if root_cfg.REVIEW_MODE_FLAG.exists():
            root_cfg.REVIEW_MODE_FLAG.unlink()

    def review_mode(self) -> None:
        """Manage entering and exiting review mode.
        The intent of review mode is to help with manual review of sensor data.
        The actual implementation is up to the Sensor subclass.
        But, for example, the video sensor saves images to the cloud every few seconds with the same
        name so that the user positioning a camera can see what the camera is seeing in near-real time.
        I2C sensors will typically increase their logging frequency to help with manual review.

        Review mode automatically exits after a timeout period (e.g. 30 minutes) to avoid cases
        where the device is unexpectedly left in review mode.

        Review mode is set via the BCLI. The BCLI also helps the user understand how to see the
        output from the sensors in review mode.
        """
        click.echo(f"{dash_line}")
        click.echo("# REVIEW MODE")
        click.echo("The intent of review mode is to help with manual review of sensor data.")
        click.echo("For example, the video sensor saves images to the cloud every few seconds so that")
        click.echo("the user positioning a camera can see what the camera is seeing in near-real time. ")
        click.echo("I2C sensors will typically increase their logging frequency.")
        click.echo("\nReview mode automatically exits after a timeout period (e.g. 30 minutes).")
        click.echo("However you would ideally exit review mode manually using this CLI.\n")
        click.echo(f"{dash_line}\n")

        # Check the flag to see if we're already in review mode
        if root_cfg.REVIEW_MODE_FLAG.exists():
            click.echo("Device is currently in review mode.")
            click.echo("Do you want to exit review mode? (Y/N)")
            char = click.getchar().lower()
            if char == "y":
                click.echo("Exiting review mode...")
                self.exit_review_mode()
                click.echo("Review mode exited.")
                return
        else:
            click.echo("Do you want to enter review mode? (Y/N)")
            char = click.getchar().lower()
            if char == "y":
                click.echo("Entering review mode")
                self.enter_review_mode()
                click.echo("Review mode may take up to ~10mins to become active.")
                click.echo("As an alternative, you can restart the device to enter review mode immediately.")
            else:
                click.echo("Exiting without entering review mode.")
                return

        # Offer the user the option to watch the sensor logs in real time
        click.echo("Do you want to monitor the output from the sensors now they're in review mode? (Y/N)")
        char = click.getchar().lower()
        if char == "y":
            click.echo("Monitoring sensor output...")
            # Start monitoring sensor output by calling the display_sensor_logs function
            # Strip everything before "Save log:" and exclude SCORE and SCORP logs
            run_cmd_live_echo(
                "journalctl -f | grep 'Save log:' | grep -v SCORE | grep -v SCORP | sed 's/.*Save log: //'"
            )
        else:
            click.echo("Exiting.")
            return

    def show_crontab_entries(self) -> None:
        """Display the crontab entries for the user."""
        click.echo(f"{dash_line}")
        click.echo("# CRONTAB ENTRIES")
        click.echo(f"{dash_line}\n")
        if not root_cfg.running_on_rpi:
            click.echo("This command only works on a Raspberry Pi")
            return
        # Get the crontab entries for the user 'bee-ops'
        cron = CronTab(user=utils.get_current_user())
        for job in cron:
            click.echo(job)
        click.echo("\n")

    def reboot_device(self) -> None:
        """Reboot the device."""
        if not root_cfg.running_on_rpi:
            click.echo("This command only works on a Raspberry Pi")
            return
        click.echo("Are you sure you want to reboot the device? (y/n)")
        if click.getchar().lower() != "y":
            click.echo("Reboot cancelled.")
            return
        click.echo("Rebooting the device...")
        run_cmd_live_echo("sudo reboot")

    def update_storage_key(self) -> None:
        """Update the storage key in ~/.expidite/keys.env."""
        # Ask the user for the new storage key
        click.echo(
            "This option enables you to update the SAS key "
            "for access to your Azure cloud storage. "
            "This is normal practice when you are going to use this device for a new experiment. "
            "You'll find the SAS key in portal.azure.com > Storage accounts > "
            "Security + networking > Shared Access Signature (SAS)."
            "\nIt should look something like:\n"
            "'DefaultEndpointsProtocol=https;AccountName=mystorageaccount;"
            "AccountKey=UnZzSivXKjXl0NffCODRGqNDFGCwSBHDG1UcaIeGOdzo2zfFs45"
            "GXTB9JjFfD/ZDuaLH8m3te6+ASt2HoD+w==;EndpointSuffix=core.windows.net;'\n"
        )
        click.echo("Enter the new storage key:")
        # Strip any leading or trailing whitespace or " or ' characters so we can handle users either wrapping
        # with quotes or not
        new_key = input()
        new_key = new_key.strip().strip('"').strip("'")
        # Check the key is not empty and contains "core.windows.net"
        if not new_key or "core.windows.net" not in new_key:
            click.echo("That doesn't look like a valid key. Please try again.")
            return

        # Read existing file and preserve all other lines.
        existing_lines = []

        if root_cfg.KEYS_FILE.exists():
            with open(root_cfg.KEYS_FILE) as f:
                for line in f:
                    if line.strip().startswith("cloud_storage_key="):
                        existing_lines.append(f'cloud_storage_key="{new_key}"\n')
                    else:
                        existing_lines.append(line)

        # Check the key is valid by trying to create a CloudConnector instance.
        try:
            test_file = root_cfg.KEYS_FILE.with_suffix(".test")
            if test_file.exists():
                test_file.unlink()

            with open(test_file, "w") as f:
                for line in existing_lines:
                    f.write(line)

            cc = CloudConnector.get_instance(root_cfg.CloudType.AZURE)
            cc.set_keys(keys_file=test_file)
            cc.list_cloud_files(root_cfg.my_device.cc_for_fair)
            click.echo("Storage key test passed.")
        except Exception as e:
            click.echo(f"Storage key test failed: {e}")
            if test_file.exists():
                test_file.unlink()
            return

        # Backup and replace.
        click.echo(f"Saving old file as {root_cfg.KEYS_FILE.with_suffix('.bak')}")
        if root_cfg.KEYS_FILE.exists():
            root_cfg.KEYS_FILE.rename(root_cfg.KEYS_FILE.with_suffix(".bak"))
        click.echo(f"Updating the storage key in {root_cfg.KEYS_FILE}")
        test_file.rename(root_cfg.KEYS_FILE)

    def nmap_ping_scan(self) -> None:
        """Run an nmap ping scan of the local network."""
        # Check if nmap is installed
        if run_cmd("which nmap").strip() == "":
            click.echo("nmap is not installed. Installing...")
            run_cmd_live_echo("sudo apt-get update && sudo apt-get install -y nmap")
            if run_cmd("which nmap").strip() == "":
                click.echo("nmap installation failed. Exiting...")
                return
        click.echo("Running nmap ping scan of local network...")
        # Get the local IP address and subnet mask
        local_ip = run_cmd("hostname -I").strip().split()[0]
        # Run as root in order to get MAC addresses
        output = run_cmd_live_echo(f"sudo nmap -sn {local_ip}/24")
        click.echo("Nmap ping scan completed.")
        click.echo("Nmap output:")
        click.echo(output)
        click.echo("\n")

    ##########################################################################################################
    # Testing menu functions
    ##########################################################################################################
    def validate_device(self) -> None:
        """Validate the device by running a series of tests."""
        click.echo(f"{dash_line}")
        click.echo("# VALIDATE DEVICE")
        click.echo(f"{dash_line}")

        success = True

        if not root_cfg.system_cfg.is_valid:
            click.echo("ERROR: System.cfg is not set. Please check your installation.")
            return

        # Check that rpi-connect is running
        try:
            if root_cfg.running_on_rpi:
                output = run_cmd("rpi-connect status")
                if "Signed in: yes" in output:
                    click.echo("\nrpi-connect is running.")
                else:
                    click.echo(
                        "\nERROR: rpi-connect is not running. Please start it using the maintenance menu."
                    )
                    success = False

            # Check that the devices configured are working
            sensors: dict[str, list[int]] = {}
            edge_orch = EdgeOrchestrator.get_instance()
            edge_orch.load_config()
            if edge_orch is not None:
                for _i, dptree in enumerate(edge_orch.dp_trees):
                    sensor_cfg = dptree.sensor.config
                    sensors.setdefault(sensor_cfg.sensor_type.value, []).append(sensor_cfg.sensor_index)
            if sensors:
                click.echo("\nSensors configured:")
                for sensor_type, indices in sensors.items():
                    click.echo(f"  {sensor_type}: {', '.join(map(str, indices))}")
            else:
                click.echo("\nNo sensors configured.")

            if api.SENSOR_TYPE.CAMERA.value in sensors:
                # Validate that the camera(s) is working
                click.echo(f"\nCameras expected for indices: {sensors[api.SENSOR_TYPE.CAMERA.value]}")
                camera_indexes = sensors[api.SENSOR_TYPE.CAMERA.value]
                for index in camera_indexes:
                    camera_test_result = run_cmd(f"rpicam-hello --camera {index}").lower()
                    if "no camera" in camera_test_result:
                        click.echo(f"ERROR: Camera {index} not found.")
                        success = False
                    else:
                        click.echo(f"Camera {index} is working.")

            if api.SENSOR_TYPE.USB.value in sensors:
                # Validate that the USB audio device(s) is working
                num_usb_devices = len(sensors[api.SENSOR_TYPE.USB.value])
                click.echo(f"\nUSB devices expected for indices... {num_usb_devices}")
                click.echo(run_cmd("lsusb"))

                # Assume all USB devices are microphones
                sound_test = run_cmd("find /sys/devices/ -name id | grep usb | grep sound")
                # Count the number of instances of "sound" in the output
                sound_count = sound_test.count("sound")
                if sound_count == num_usb_devices:
                    click.echo("\nFound the correct number of USB audio device(s).")
                    click.echo(sound_test)
                else:
                    click.echo(
                        f"\nERROR: Found {sound_count} USB audio device(s), but expected {num_usb_devices}."
                    )
                    success = False

            if api.SENSOR_TYPE.I2C.value in sensors:
                # Validate that the I2C device(s) is working
                click.echo(f"\nI2C devices expected for indices: {sensors[api.SENSOR_TYPE.I2C.value]}")
                i2c_indexes = sensors[api.SENSOR_TYPE.I2C.value]
                i2c_test_result = run_cmd("i2cdetect -y 1")
                for index in i2c_indexes:
                    # We need to convert the index from base10 to base16
                    hex_index = f"{index:X}"
                    if str(hex_index) in i2c_test_result:
                        click.echo(f"I2C device {index} ({hex_index}) is working.")
                    else:
                        click.echo(f"ERROR: I2C device {index} ({hex_index}) not found.")
                        click.echo(i2c_test_result)
                        success = False

            # Check for RAISE_WARNING tag in logs
            if root_cfg.running_on_rpi:
                since_time = api.utc_now() - timedelta(hours=4)
                logs = device_health.get_logs(since=since_time, min_priority=4, grep_str=["RAISE_WARNING"])
                if logs:
                    success = False
                    click.echo("\nRAISE_WARNING tag found in logs:")
                    for log in logs:
                        click.echo(f"{log['time_logged']} - {log['priority']} - {log['message']}")
                else:
                    click.echo("\nNo error logs found.")

        except Exception as e:
            logger.exception(f"{root_cfg.RAISE_WARN()}Exception running validation tests")
            click.echo(f"ERROR: exception running validation tests: {e}")
            success = False

        if success:
            click.echo("\n ### PASS ###\n")
        else:
            click.echo("\n ### FAIL ###\n")

        # Now flash the LED green and then red
        with open(root_cfg.LED_STATUS_FILE, "w") as f:
            f.write("red:blink:0.25")  # Flash red
            time.sleep(2)
            f.write("green:blink:0.25")  # Flash green
            time.sleep(2)

        click.echo(f"{dash_line}")

    def run_network_test(self) -> None:
        """Run a network test and display the results."""
        click.echo(f"{dash_line}")
        click.echo("# NETWORK INFO")
        click.echo(f"{dash_line}")
        if not root_cfg.running_on_rpi:
            click.echo("This command only works on a Raspberry Pi")
            return
        if not root_cfg.system_cfg.is_valid:
            click.echo("System.cfg is not set. Please check your installation.")
            return
        scripts_dir = Path.home() / root_cfg.system_cfg.venv_dir / "scripts"
        if not scripts_dir.exists():
            click.echo(
                f"Error: scripts directory does not exist at {scripts_dir}. Please check your installation."
            )
            return
        run_cmd_live_echo(f"sudo {scripts_dir}/network_test.sh q")
        click.echo(f"{dash_line}\n")

    ##########################################################################################################
    # Interactive menu functions
    ##########################################################################################################
    def interactive_menu(self) -> None:
        """Interactive menu for navigating commands."""
        # click.clear()

        # Check if we need to setup keys or git repo or inventory
        check_if_setup_required()

        # Display status
        click.echo(f"{dash_line}")
        click.echo(f"# Expidite CLI on {root_cfg.my_device_id} {root_cfg.my_device.name}")
        while True:
            click.echo(f"{header}Main Menu:")
            click.echo("0. Exit")
            click.echo("1. View Config")
            click.echo("2. View Status")
            click.echo("3. Validate device")
            click.echo("4. Sensing Commands")
            click.echo("5. Maintenance Commands")
            click.echo("6. Debug Commands")
            try:
                choice = click.prompt("\nEnter your choice", type=int, default=0)
                click.echo("\n")
            except ValueError:
                click.echo("Invalid input. Please enter a number.")
                continue

            if choice == 1:
                self.view_rpi_core_config()
            elif choice == 2:
                self.view_status()
            elif choice == 3:
                self.validate_device()
            elif choice == 4:
                self.sensing_menu()
            elif choice == 5:
                self.maintenance_menu()
            elif choice == 6:
                self.debug_menu()
            elif choice == 0:
                click.echo("Exiting...")
                break
            else:
                click.echo("Invalid choice. Please try again.")
        # Clean up and exit
        cc = CloudConnector.get_instance(root_cfg.CloudType.AZURE)
        assert isinstance(cc, AsyncCloudConnector)
        cc.shutdown()

    def sensing_menu(self) -> None:
        """Menu for sensing commands."""
        while True:
            click.echo(f"{header}Sensing Menu:")
            click.echo("0. Back to Main Menu")
            click.echo("1. Trigger sensing operation now")
            try:
                choice = click.prompt("\nEnter your choice", type=int, default=0)
                click.echo("\n")
            except ValueError:
                click.echo("Invalid input. Please enter a number.")
                continue

            if choice == 1:
                self.trigger_sensing()
            elif choice == 0:
                break
            else:
                click.echo("Invalid choice. Please try again.")

    def debug_menu(self) -> None:
        """Menu for debugging commands."""
        while True:
            click.echo(f"{header}Debug Menu:")
            click.echo("0. Back to Main Menu")
            click.echo("1. Run Network Test")
            click.echo("2. Display logs live (journalctl)")
            click.echo("3. Display errors")
            click.echo("4. Display all expidite logs")
            click.echo("5. Display sensor measurement logs")
            click.echo("6. Display SCORE sensor activity logs")
            click.echo("7. Display running processes")
            click.echo("8. Show recordings and data files")
            click.echo("9. Show Crontab Entries")
            click.echo("10. nmap ping scan of local network")
            try:
                choice = click.prompt(
                    "\nEnter your choice",
                    type=int,
                    default=0,
                )
                click.echo("\n")
            except ValueError:
                click.echo("Invalid input. Please enter a number.")
                continue

            if choice == 1:
                self.run_network_test()
            elif choice == 2:
                self.journalctl()
            elif choice == 3:
                self.display_errors()
            elif choice == 4:
                self.display_rpi_core_logs()
            elif choice == 5:
                self.display_sensor_logs()
            elif choice == 6:
                self.display_score_logs()
            elif choice == 7:
                self.display_running_processes()
            elif choice == 8:
                self.show_recordings()
            elif choice == 9:
                self.show_crontab_entries()
            elif choice == 10:
                self.nmap_ping_scan()
            elif choice == 0:
                break
            else:
                click.echo("Invalid choice. Please try again.")

    def maintenance_menu(self) -> None:
        """Menu for maintenance commands."""
        while True:
            click.echo(f"{header}Maintenance Menu:")
            click.echo("0. Back to Main Menu")
            click.echo("1. Update Software")
            click.echo("2. Enable rpi-connect")
            click.echo("3. Review mode")
            click.echo("4. Start RpiCore")
            click.echo("5. Stop RpiCore (graceful stop)")
            click.echo("6. Hard stop RpiCore (pkill)")
            click.echo("7. Reboot the Device")
            click.echo("8. Update storage key")
            try:
                choice = click.prompt("\nEnter your choice", type=int, default=0)
                click.echo("\n")
            except ValueError:
                click.echo("Invalid input. Please enter a number.")
                continue

            if choice == 1:
                self.update_software()
            elif choice == 2:
                self.enable_rpi_connect()
            elif choice == 3:
                self.review_mode()
            elif choice == 4:
                self.start_rpi_core()
            elif choice == 5:
                self.stop_rpi_core(pkill=False)
            elif choice == 6:
                self.stop_rpi_core(pkill=True)
            elif choice == 7:
                self.reboot_device()
            elif choice == 8:
                self.update_storage_key()
            elif choice == 0:
                break
            else:
                click.echo("Invalid choice. Please try again.")


##############################################################################################################
# Main function to run the CLI
# Main just calls the interactive menu
##############################################################################################################
def main() -> None:
    # Disable console logging during CLI execution
    with disable_console_logging("expidite"):
        try:
            im = InteractiveMenu()
            im.interactive_menu()
        except (KeyboardInterrupt, click.exceptions.Abort):
            click.echo("\nExiting...")
        except Exception as e:
            logger.exception("Error in CLI")
            click.echo(f"Error in CLI: {e}")
        finally:
            # Ensure the cloud connector is shut down
            cc = CloudConnector.get_instance(root_cfg.CloudType.AZURE)
            assert isinstance(cc, AsyncCloudConnector)
            cc.shutdown()
            click.echo("Done")


if __name__ == "__main__":
    os.chdir(root_cfg.HOME_DIR)
    main()

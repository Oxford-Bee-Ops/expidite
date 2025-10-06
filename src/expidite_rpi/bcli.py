####################################################################################################
# Description: This script is used to run the bcli command.
####################################################################################################
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

###################################################################################################
# Utility functions
###################################################################################################

# Wrapper for utils.run_cmd so that we can display error rather than throwing an exception
def run_cmd(cmd: str) -> str:
    """Run a command and return its output or an error message."""
    if not root_cfg.running_on_rpi:
        return "This command only works on a Raspberry Pi"
    try:
        return utils.run_cmd(cmd, ignore_errors=True)
    except Exception as e:
        return f"Error: {e}"

def run_grep(cmd: str) -> str:
    """Run a grep command and return its output or an error message.
    We need to allow a series of |d commands to be run, so we use bash -c"""
    if not root_cfg.running_on_rpi:
        return "This command only works on a Raspberry Pi"
    try:
        return utils.run_cmd(f"bash -c \"{cmd}\"", ignore_errors=True)
    except Exception as e:
        return f"Error: {e}"

def reader(proc: subprocess.Popen, queue: queue.Queue) -> None:
    """
    Read 'stdout' from the subprocess and put it into the queue.

    Args:
        proc: The subprocess to read from.
        queue: The queue to store the output lines.
    """
    if proc.stdout:
        for line in iter(proc.stdout.readline, b""):
            queue.put(line)


def run_cmd_live_echo(cmd: str) -> str:
    """
    Run a command and echo its output in real-time.

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
    """Check if setup is required by verifying keys."""
    attempts = 0
    max_attempts = 3
    while not check_keys_env():
        attempts += 1
        if attempts >= max_attempts:
            click.echo("Setup not completed. Exiting...")
            sys.exit(1)
        click.echo("Press any key to retry setup...")
        click.getchar()


def check_keys_env() -> bool:
    """
    Check if the keys.env exists in ./rpi_core and is not empty.

    Returns:
        True if the keys.env file exists and is valid, False otherwise.
    """
    success, error = root_cfg.check_keys()
    if success:
        return True
    else:    
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
        click.echo("# cloud_storage_key=\"DefaultEndpointsProtocol=https;AccountName=mystorageprod;"
                   "AccountKey=UnZzSivXKjXl0NffCODRGqNDFGCwSBHDG1UcaIeGOdzo2zfFs45GXTB9JjFfD/"
                   "ZDuaLH8m3tf6+ASt2HoD+w==;EndpointSuffix=core.windows.net;\"")
        click.echo("# ")
        click.echo("# Press any key to continue once you have done so")
        click.echo("# ")
        click.echo(f"{dash_line}")
        return False

class InteractiveMenu():
    """Interactive menu for navigating commands."""
    def __init__(self):
        self.sc = RpiCore()
        inventory = root_cfg.load_configuration()
        logger.debug(f"Inventory: {inventory}")
        if inventory:
            self.sc.configure(inventory)

    ####################################################################################################
    # Main menu functions
    ####################################################################################################
    def view_status(self) -> None:
        """View the current status of the device."""
        try:
            click.echo(self.sc.status(verbose=False))
            self.display_score_logs()
            self.display_sensor_logs()
        except Exception as e:
            click.echo(f"Error in script start up: {e}")


    def view_rpi_core_config(self) -> None:
        """View the rpi core configuration."""
        # Check we have bloc storage access
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
                click.echo(f"{i}> {sensor_cfg.sensor_type} {sensor_cfg.sensor_index} "
                           f" {sensor_cfg.sensor_model}")
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



    ####################################################################################################
    # Debug menu functions
    ####################################################################################################
    def journalctl(self) -> None:
        """Continuously display journal logs in real time."""
        # Ask if the user wants to specify a grep filter
        click.echo("Do you want to filter the logs? (y/n)")
        char = click.getchar()
        click.echo(char)
        if char == "y":
            click.echo("Enter the grep filter string:")
            filter = input()
        else:
            filter = ""
        click.echo("Press Ctrl+C to exit...\n")
        if root_cfg.running_on_windows:
            click.echo("This command only works on Linux. Exiting...")
            return
        try:
            if filter != "":
                process = subprocess.Popen(
                    ["journalctl", "-f", "|", "grep -i", filter],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
            else:
                process = subprocess.Popen(["journalctl", "-f"], 
                                           stdout=subprocess.PIPE, 
                                           stderr=subprocess.PIPE)
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
            click.echo(f"{log['timestamp']} - {log['priority']} - {log['message']}")

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
        utils.run_cmd("journalctl",
                      ignore_errors=True,
                      grep_strs=["error", "fail", "critical", "panic"])

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
        click.echo("# Displaying sensor output logs from the last 30 minutes")
        click.echo(f"{dash_line}")
        since_time = api.utc_now() - timedelta(minutes=30)
        logs = device_health.get_logs(since=since_time, min_priority=6, grep_str=[api.TELEM_TAG])
        self.display_logs(logs)


    def display_score_logs(self) -> None:
        """View the SCORE logs."""
        if root_cfg.running_on_windows:
            click.echo("This command only works on Linux. Exiting...")
            return
        click.echo(self.sc.status(verbose=False))
        click.echo(f"\n{dash_line}")
        click.echo("# Expidite SCORE logs of sensor output")
        click.echo(f"{dash_line}")
        since_time = api.utc_now() - timedelta(minutes=30)
        logs = device_health.get_logs(since=since_time, min_priority=6, 
                                      grep_str=[api.TELEM_TAG, "SCORE"])
        self.display_logs(logs)
        click.echo(f"{dash_line}\n")


    def display_running_processes(self) -> None:
        # Running processes
        # Drop any starting / or . characters
        # And convert the process list to a simple comma-seperated string with no {} or ' or " 
        # characters
        if root_cfg.system_cfg is None:          
            click.echo("System.cfg is not set. Please check your installation.")
            return
        process_set = (
            utils.check_running_processes(search_string=f"{root_cfg.system_cfg.my_start_script}")
        )
        process_list_str = (
            str(process_set).replace("{", 
                                     "").replace("}", "").replace("'", "").replace('"', "").strip()
        )
        click.echo(f"{dash_line}")
        click.echo("# Display running RpiCore processes")
        click.echo(f"{dash_line}\n")
        click.echo(process_list_str)

        # Also display the count of live sensor and dptree threads.
        since_time = api.utc_now() - timedelta(minutes=30)
        logs = device_health.get_logs(since=since_time, min_priority=6, grep_str=["Sensor threads alive"])
        if logs:
            click.echo(logs[-1]["message"])
        logs = device_health.get_logs(since=since_time, min_priority=6, grep_str=["DPtrees alive"])
        if logs:
            click.echo(logs[-1]["message"])


    def show_recordings(self) -> None:
        # List all files under the root_working_dir
        click.echo(f"{dash_line}")
        click.echo("# RpiCore recordings")
        click.echo(f"{dash_line}")
        click.echo("Recording files:")
        click.echo(run_cmd(f"ls -lhR {root_cfg.ROOT_WORKING_DIR}*"))
        click.echo("\n")

    ####################################################################################################
    # Maintenance menu functions
    ####################################################################################################
    def update_software(self) -> None:
        """Update the software to the latest version."""
        click.echo("Running update to get latest code...")
        if root_cfg.running_on_windows:
            click.echo("This command only works on Linux. Exiting...")
            return
        # Check if the scripts directory exists
        if root_cfg.system_cfg is None:
            click.echo("System.cfg is not set. Please check your installation.")
            return
        scripts_dir = Path.home() / root_cfg.system_cfg.venv_dir / "scripts"
        if scripts_dir.exists():
            run_cmd_live_echo(f"sudo -u $USER {scripts_dir}/rpi_installer.sh")
        else:
            click.echo(f"Error: scripts directory does not exist at {scripts_dir}. "
                       f"Please check your installation.")
            return


    def start_rpi_core(self) -> None:
        """Start the RpiCore service."""
        click.echo("Starting RpiCore...")

        # If my_start_script is a resolvable module in this environment, then we use that to start the service
        # using that user-provided script.
        if (root_cfg.system_cfg is None or 
            root_cfg.system_cfg.my_start_script is None or
            root_cfg.system_cfg.my_start_script == root_cfg.FAILED_TO_LOAD):
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
                # This will raise an ImportError if the module is not found
                # or if the main() function is not defined in the module
                module = __import__(my_start_script, fromlist=["main"])
                main_func = getattr(module, "main", None)
                if main_func is None:
                    click.echo(f"main() function not found in {my_start_script}")
                    click.echo("Exiting...")
                    return
            except ImportError as e:
                logger.error(f"{root_cfg.RAISE_WARN()}Module {my_start_script} not resolvable ({e})", 
                             exc_info=True)
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
        return


    def stop_rpi_core(self, pkill: bool) -> None:
        """Stop the RpiCore service."""
        click.echo("Stopping RpiCore... this may take up to 180s to complete.")
        # We just need to "touch" the stop file to stop the service
        root_cfg.STOP_EXPIDITE_FLAG.touch()

        if pkill and root_cfg.system_cfg:
                run_cmd(f"sudo pkill -f 'python -m {root_cfg.system_cfg.my_start_script}'")
        return


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
        click.echo("This option enables you to update the SAS key "
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
        # Strip any leading or trailing whitespace or " or ' characters so we can handle
        # users either wrapping with quotes or not
        new_key = input()
        new_key = new_key.strip().strip('"').strip("'")

        # Check the key is not empty and contains "core.windows.net"
        if not new_key or "core.windows.net" not in new_key:
            click.echo("That doesn't look like a valid key. Please try again.")
            return


        # Check the key is valid by trying to create a CloudConnector instance
        try:
            test_file = root_cfg.KEYS_FILE.with_suffix(".test")
            if test_file.exists():
                test_file.unlink()
            with open(test_file, "w") as f:
                f.write(f"cloud_storage_key=\"{new_key}\"\n")
            cc = CloudConnector.get_instance(root_cfg.CloudType.AZURE)
            cc.set_keys(keys_file=test_file)
            cc.list_cloud_files(root_cfg.my_device.cc_for_fair)
            click.echo("Storage key test passed.")
        except Exception as e:
            click.echo(f"Storage key test failed: {e}")
            test_file.unlink()
            return
       
        click.echo(f"Saving old file as {root_cfg.KEYS_FILE.with_suffix('.bak')}")
        root_cfg.KEYS_FILE.rename(root_cfg.KEYS_FILE.with_suffix(".bak"))

        click.echo(f"Updating the storage key in {root_cfg.KEYS_FILE}")
        test_file.rename(root_cfg.KEYS_FILE)

        
    ####################################################################################################
    # Testing menu functions
    ####################################################################################################
    LED_STATUS_FILE: Path = Path(os.environ.get("LED_STATUS_FILE", "/.expidite/flags/led_status"))
    def validate_device(self) -> None:
        """Validate the device by running a series of tests."""
        click.echo(f"{dash_line}")
        click.echo("# VALIDATE DEVICE")
        click.echo(f"{dash_line}")

        success = True

        if root_cfg.system_cfg is None:
            click.echo("ERROR: System.cfg is not set. Please check your installation.")
            return
        
        # Check that rpi-connect is running
        try:
            if root_cfg.running_on_rpi:
                output = run_cmd("rpi-connect status")
                if ("Signed in: yes" in output):
                    click.echo("\nrpi-connect is running.")
                else:
                    click.echo("\nERROR: rpi-connect is not running. Please start it using the "
                            "maintenance menu.")
                    success = False

            # Check that the devices configured are working
            sensors: dict[str, list[int]] = {}
            edge_orch = EdgeOrchestrator.get_instance()
            edge_orch.load_config()
            if edge_orch is not None:
                for i, dptree in enumerate(edge_orch.dp_trees):
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
                    camera_test_result = run_cmd(f"libcamera-hello --cameras {index}")
                    if "camera is not available" in camera_test_result:
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
                    click.echo(f"\nERROR: Found {sound_count} USB audio device(s), "
                            f"but expected {num_usb_devices}.")
                    success = False

            if api.SENSOR_TYPE.I2C.value in sensors:
                # Validate that the I2C device(s) is working
                click.echo(f"\nI2C devices expected for indices: {sensors[api.SENSOR_TYPE.I2C.value]}")
                i2c_indexes = sensors[api.SENSOR_TYPE.I2C.value]
                i2c_test_result = run_cmd("i2cdetect -y 1")
                for index in i2c_indexes:
                    # We need to convert the index from base10 to base16
                    hex_index = hex(index)[2:].upper()
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
            logger.error(f"{root_cfg.RAISE_WARN()}Exception running validation tests: {e}", exc_info=True)
            click.echo(f"ERROR: exception running validation tests: {e}")
            success = False

        if success:
            click.echo("\n ### PASS ###\n")
        else:
            click.echo("\n ### FAIL ###\n")

        # Now flash the LED green and then red
        LED_STATUS_FILE: Path = Path.home() / ".expidite" / "flags" / "led_status"

        if not LED_STATUS_FILE.exists():
            LED_STATUS_FILE.parent.mkdir(parents=True, exist_ok=True)
            LED_STATUS_FILE.touch()
        with open(LED_STATUS_FILE, "w") as f:
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
        if root_cfg.system_cfg is None:
            click.echo("System.cfg is not set. Please check your installation.")
            return
        scripts_dir = Path.home() / root_cfg.system_cfg.venv_dir / "scripts"
        if not scripts_dir.exists():
            click.echo(f"Error: scripts directory does not exist at {scripts_dir}. "
                       f"Please check your installation.")
            return
        run_cmd_live_echo(f"sudo {scripts_dir}/network_test.sh q")
        click.echo(f"{dash_line}")

    def run_system_test(self) -> None:
        """Invokes my_start_script."""
        # Check this is a system_test installation
        if root_cfg.system_cfg is None:
            click.echo("System.cfg is not set. Please check your installation.")
            return
        if root_cfg.system_cfg.install_type != api.INSTALL_TYPE.SYSTEM_TEST:
            click.echo("This command only works on a system test installation.")
            return
        if root_cfg.system_cfg.my_start_script is not None:
            click.echo(f"Running {root_cfg.system_cfg.my_start_script}...")
            # my_start_script should be a resolvable module in this environment
            try:
                my_start_script = root_cfg.system_cfg.my_start_script
                # Try creating an instance and calling main()
                # This will raise an ImportError if the module is not found
                # or if the main() function is not defined in the module
                module = __import__(my_start_script, fromlist=["main"])
                main_func = getattr(module, "main", None)
                if main_func is None:
                    click.echo(f"main() function not found in {my_start_script}")
                    click.echo("Exiting...")
                    return
            except ImportError as e:
                logger.error(f"{root_cfg.RAISE_WARN()}Module {my_start_script} not resolvable ({e})", 
                             exc_info=True)
                click.echo(f"Module {my_start_script} not resolvable ({e})")
                click.echo("Exiting...")
                return
            else:
                if root_cfg.running_on_windows:
                    # Invoke the module directly from the current thread.
                    click.echo(f"Found {my_start_script}. Running system test...")
                    # Call the main function directly
                    try:
                        main_func()
                    except Exception as e:
                        click.echo(f"Error running {my_start_script}: {e}")
                        return
                elif root_cfg.running_on_rpi:
                    click.echo(f"Found {my_start_script}. Running system test as a background process...")
                    cmd = (
                        f"bash -c 'source {root_cfg.HOME_DIR}/{root_cfg.system_cfg.venv_dir}/bin/activate && "
                        f"nohup python -m {my_start_script} 2>&1 | /usr/bin/logger -t EXPIDITE &'"
                    )
                    click.echo(f"Running command: {cmd}")
                    run_cmd_live_echo(cmd)

                    
    ####################################################################################################
    # Interactive menu functions
    ####################################################################################################
    def interactive_menu(self) -> None:
        """Interactive menu for navigating commands."""
        #click.clear()

        # Check if we need to setup keys or git repo
        check_if_setup_required()

        # Display status
        click.echo(f"{dash_line}")
        click.echo(f"# RpiCore CLI on {root_cfg.my_device_id} {root_cfg.my_device.name}")
        while True:
            click.echo(f"{header}Main Menu:")
            click.echo("0. Exit")
            click.echo("1. View Config")
            click.echo("2. View Status")
            click.echo("3. Maintenance Commands")
            click.echo("4. Debugging Commands")
            click.echo("5. Testing Commands")
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
                self.maintenance_menu()
            elif choice == 4:
                self.debug_menu()
            elif choice == 5:
                self.testing_menu()
            elif choice == 0:
                click.echo("Exiting...")
                break
            else:
                click.echo("Invalid choice. Please try again.")
        # Clean up and exit
        cc = CloudConnector.get_instance(type=root_cfg.CloudType.AZURE)
        assert isinstance(cc, AsyncCloudConnector)
        cc.shutdown()


    def debug_menu(self) -> None:
        """Menu for debugging commands."""
        while True:
            click.echo(f"{header}Debugging Menu:")
            click.echo("0. Back to Main Menu")
            click.echo("1. Journalctl")
            click.echo("2. Display errors")
            click.echo("3. Display all expidite logs")
            click.echo("4. Display sensor measurement logs")
            click.echo("5. Display SCORE sensor activity logs")
            click.echo("6. Display running processes")
            click.echo("7. Show recordings and data files")
            click.echo("8. Show Crontab Entries")
            try:
                choice = click.prompt("\nEnter your choice", type=int, default=0, )
                click.echo("\n")
            except ValueError:
                click.echo("Invalid input. Please enter a number.")
                continue

            if choice == 1:
                self.journalctl()
            elif choice == 2:
                self.display_errors()
            elif choice == 3:
                self.display_rpi_core_logs()
            elif choice == 4:
                self.display_sensor_logs()
            elif choice == 5:
                self.display_score_logs()
            elif choice == 6:
                self.display_running_processes()
            elif choice == 7:
                self.show_recordings()
            elif choice == 8:
                self.show_crontab_entries()
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
            click.echo("3. Start RpiCore")
            click.echo("4. Stop RpiCore (graceful stop)")
            click.echo("5. Hard stop RpiCore (pkill)")
            click.echo("6. Reboot the Device")
            click.echo("7. Update storage key")
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
                self.start_rpi_core()
            elif choice == 4:
                self.stop_rpi_core(pkill=False)
            elif choice == 5:
                self.stop_rpi_core(pkill=True) 
            elif choice == 6: 
                self.reboot_device()
            elif choice == 7: 
                self.update_storage_key()
            elif choice == 0:
                break
            else:
                click.echo("Invalid choice. Please try again.")


    def testing_menu(self) -> None:
        """Menu for testing commands."""
        while True:
            click.echo(f"{header}Testing Menu:")
            click.echo("0. Back to Main Menu")
            click.echo("1. Validate device")
            click.echo("2. Run Network Test")
            click.echo("3. Run system test")
            try:
                choice = click.prompt("\nEnter your choice", type=int, default=0)
                click.echo("\n")
            except ValueError:
                click.echo("Invalid input. Please enter a number.")
                continue

            if choice == 1:
                self.validate_device()
            elif choice == 2:
                self.run_network_test()
            elif choice == 3:
                self.run_system_test()
            elif choice == 0:
                break
            else:
                click.echo("Invalid choice. Please try again.")
    

#################################################################################
# Main function to run the CLI
# Main just calls the interactive menu
#################################################################################
def main():
    # Disable console logging during CLI execution
    with disable_console_logging("expidite"):
        try:
            im = InteractiveMenu()
            im.interactive_menu()
        except (KeyboardInterrupt, click.exceptions.Abort):
            click.echo("\nExiting...")
        except Exception as e:
            logger.error(f"Error in CLI: {e}", exc_info=True)
            click.echo(f"Error in CLI: {e}")
        finally:
            # Ensure the cloud connector is shut down
            cc = CloudConnector.get_instance(type=root_cfg.CloudType.AZURE)
            assert isinstance(cc, AsyncCloudConnector)
            cc.shutdown()
            click.echo("Done")

if __name__ == "__main__":
    os.chdir(root_cfg.HOME_DIR)
    main()
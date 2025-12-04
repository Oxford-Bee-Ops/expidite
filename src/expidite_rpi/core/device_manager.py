# Class to optionally manage:
# - Wifi
# - LED indicator status
import os
from time import sleep

from expidite_rpi.core import api
from expidite_rpi.core import configuration as root_cfg
from expidite_rpi.core.diagnostics_bundle import DiagnosticsBundle
from expidite_rpi.utils import utils

logger = root_cfg.setup_logger("expidite")


class DeviceManager:
    """Manages LED & Wifi status if configured to do so in my_device (DeviceCfg):
    - manage_wifi: bool = True
    - manage_leds: bool = True
    """

    # Device states
    S_BOOTING = "Booting"
    S_WIFI_UP = "Wifi Up"
    S_INTERNET_UP = "Internet Up"
    S_WIFI_FAILED = "Wifi failed"
    S_AP_DOWN = "AP Down"
    S_AP_UP = "AP Up"
    S_AP_IN_USE = "AP In Use"

    def __init__(self) -> None:
        if root_cfg.system_cfg is None:
            logger.error(f"{root_cfg.RAISE_WARN()}DeviceManager: system_cfg is None; exiting")
            return
        self.ping_failure_count_all = 0
        self.ping_success_count_all = 0
        self.ping_failure_count_run = 0
        self.ping_success_count_run = 0
        self.led_timer: utils.RepeatTimer | None = None
        self.wifi_timer: utils.RepeatTimer | None = None
        self.diagnostics_upload_timer: utils.RepeatTimer | None = None

    def start(self) -> None:
        """Start the DeviceManager threads."""
        ###############################
        # Wifi management
        ###############################
        self.ping_failure_count_all = 0
        self.ping_success_count_all = 0
        self.ping_failure_count_run = 0
        self.ping_success_count_run = 0
        self.last_ping_was_ok = False
        self.log_counter = 0
        self.wifi_log_frequency = 60 * 10
        self.client_wlan = "wlan0"
        if root_cfg.my_device.wifi_clients:
            self.inject_wifi_clients()

        # Start wifi management thread
        if root_cfg.running_on_rpi and root_cfg.my_device.attempt_wifi_recovery:
            self.wifi_timer = utils.RepeatTimer(interval=2.0, function=self.wifi_timer_callback)
            self.wifi_timer.start()
            logger.info("DeviceManager Wifi timer started")

        ###############################
        # LED status management
        ###############################
        self.currentState = self.S_BOOTING
        self.currentAPState = self.S_AP_DOWN
        self.lastStateChangeTime = api.utc_now()
        self.red_led = False
        self.green_led = False
        # Start the LED management thread
        if (
            root_cfg.running_on_rpi
            and root_cfg.system_cfg is not None
            and root_cfg.system_cfg.manage_leds == "Yes"
        ):
            self.led_timer = utils.RepeatTimer(interval=5, function=self.led_timer_callback)
            self.led_timer.start()
            logger.info("DeviceManager LED timer started")

        if root_cfg.running_on_rpi:
            # We don't need to check often. These diagnostics are generated rarely and are not required in
            # real time.
            self.diagnostics_upload_timer = utils.RepeatTimer(
                interval=3600, function=self.diagnostics_upload_timer_callback
            )
            self.diagnostics_upload_timer.start()
            logger.info("Diagnostics Upload timer started")

    def stop(self) -> None:
        """Stop the DeviceManager threads."""
        if self.led_timer is not None:
            self.led_timer.cancel()
            logger.info("DeviceManager LED timer stopped")
        if self.wifi_timer is not None:
            self.wifi_timer.cancel()
            logger.info("DeviceManager Wifi timer stopped")
        if self.diagnostics_upload_timer is not None:
            self.diagnostics_upload_timer.cancel()
            logger.info("Diagnostics Upload timer stopped")

    #############################################################################################################
    # LED management functions
    #
    # LED stat FSM table
    #
    # State:            |Booting	 |Wifi up	    |Internet up	|Wifi failed
    # LED  :            |Red*        |Green*	    |Green	        |Red**
    # Input[WifiUp]	    |Wifi up	 |-	            |-              |Wifi up
    # Input[GoodPing]   |-	         |Internet up	|-	            |-
    # Input[PingFail] 	|-	         |-     	    |Wifi up	    |-
    # Input[WifiDown]	|-	         |Wifi failed	|Wifi failed
    # *=blinking
    # **=slow blinking
    #############################################################################################################

    # This function gets called every second.
    # Set the LEDs to ON or OFF as appropriate given the current device state.
    def led_timer_callback(self) -> None:
        try:
            logger.debug("LED timer callback")
            if self.currentState == self.S_BOOTING:
                # Green should be off; red should be blinking
                self.set_led_status("red", "blink:0.5")
            elif self.currentState == self.S_WIFI_UP:
                # Green should be blinking; red should be off
                self.set_led_status("green", "blink:1.0")
            elif self.currentState == self.S_INTERNET_UP:
                # Green should be on; red should be off
                self.set_led_status("green", "on")
            elif self.currentState == self.S_WIFI_FAILED:
                # Green should be off; red should be on
                self.set_led_status("red", "2.0")
        except Exception as e:
            logger.error(
                f"{root_cfg.RAISE_WARN()}LED timer callback threw an exception: " + str(e), exc_info=True
            )

    def set_led_status(self, colour: str, status: str) -> None:
        """Update the LED status file which is read by the led_control script.

        Parameters
        ----------
        - colour: "red" or "green"
        - status: "on", "off", or "blink"
        """
        try:
            with open(root_cfg.LED_STATUS_FILE, "w") as f:
                f.write(f"{colour}:{status}\n")
        except Exception as e:
            logger.error(
                f"{root_cfg.RAISE_WARN()}set_led_status threw an exception: " + str(e), exc_info=True
            )

    #############################################################################################################
    # Wifi management functions
    ##############################################################################################################
    def inject_wifi_clients(self) -> None:
        # Inject the wifi clients via nmcli
        # This is done so that the device has out-of-the-box awareness of the wifi clients
        # We use the nmcli command to get the list of wifi clients
        self.wifi_clients = root_cfg.my_device.wifi_clients

        # Use nmcli to configure the client wifi connection if it doesn't already exist
        existing_connections = utils.run_cmd("sudo nmcli -t -f NAME connection show").split("\n")

        # Inject the wifi clients
        for client in self.wifi_clients:
            if (
                client.ssid is None
                or client.priority is None
                or client.pw is None
                or client.ssid == ""
                or client.priority == 0
                or client.pw == ""
            ):
                logger.warning(f"Skipping invalid wifi client: {client}")
                continue

            # Configure the wifi client
            if client.ssid not in existing_connections:
                logger.info(f"Adding client wifi connection {client.ssid} on {self.client_wlan}")
                utils.run_cmd(
                    f"sudo nmcli connection add con-name {client.ssid} "
                    f"ifname {self.client_wlan} type wifi wifi.mode infrastructure wifi.ssid {client.ssid} "
                    f"wifi-sec.key-mgmt wpa-psk wifi-sec.psk {client.pw} "
                    f"connection.autoconnect-priority {client.priority} "
                    f"ipv4.dns '8.8.8.8 8.8.4.4'"
                )
            else:
                logger.info(f"Client wifi connection {client.ssid} already exists")

    def set_wifi_status(self, wifi_up: bool) -> None:
        if wifi_up:
            # We only check wifi status if ping has failed
            if self.currentState != self.S_WIFI_UP:
                self.currentState = self.S_WIFI_UP
                self.set_last_state_change_time()
        # Wifi failed
        elif self.currentState != self.S_WIFI_FAILED:
            self.currentState = self.S_WIFI_FAILED
            self.set_last_state_change_time()

    def set_ap_status(self, device_status: str) -> None:
        self.currentAPState = device_status

    def set_ping_status(self, ping_successful: bool) -> None:
        if ping_successful:
            # We have good connectivity to the internet
            if self.currentState != self.S_INTERNET_UP:
                self.currentState = self.S_INTERNET_UP
                self.set_last_state_change_time()
        else:
            # Ping failed, but wifi might be up
            self.set_ap_status(DeviceManager.S_AP_DOWN)
            if self.currentState == self.S_INTERNET_UP:
                self.currentState = self.S_WIFI_UP
                self.set_last_state_change_time()

    def set_last_state_change_time(self) -> None:
        self.lastStateChangeTime = api.utc_now()

    def get_time_since_last_state_change(self) -> float:
        currentTime = api.utc_now()
        return (currentTime - self.lastStateChangeTime).total_seconds()

    # Create a function for logging useful info
    def log_wifi_info(self) -> None:
        try:
            logger.info(utils.run_cmd("sudo nmcli -g SSID device wifi", ignore_errors=True))
            logger.info(utils.run_cmd("sudo ifconfig " + self.client_wlan, ignore_errors=True))
            logger.info(
                utils.run_cmd(
                    "sudo nmcli device wifi list ifname " + self.client_wlan,
                    grep_strs=["Infra"],
                    ignore_errors=True,
                )
            )
            logger.info(utils.run_cmd("sudo arp -n", ignore_errors=True))
        except Exception as e:
            # grep did not match any lines
            logger.error(f"{root_cfg.RAISE_WARN()}log_wifi_info threw an exception: " + str(e))

    # Function to manage the AP wifi connection
    # We only enable the AP wifi connection if the client wifi connection is UP
    def wifi_timer_callback(self) -> None:
        try:
            logger.debug("Wifi timer callback")
            # Test that internet connectivity is UP and working by pinging google DNS servers
            # -c 1 means ping once, -W 1 means timeout after 1 second
            ping_rc = os.system("ping -c 1 -W 1 8.8.8.8 1>/dev/null")
            if ping_rc != 0:
                self.action_on_ping_fail()
            else:
                self.action_on_ping_ok()

            # Log useful info and status periodically
            if self.log_counter % self.wifi_log_frequency == 0:
                self.log_wifi_info()
            self.log_counter += 1

        except Exception as e:
            logger.error(
                f"{root_cfg.RAISE_WARN()}Wifi timer callback threw an exception: " + str(e), exc_info=True
            )

    def action_on_ping_fail(self) -> None:
        # Track ping stats for logging purposes
        self.ping_failure_count_all += 1
        if self.last_ping_was_ok:
            self.ping_success_count_run = 0
            self.ping_failure_count_run = 0
        self.ping_failure_count_run += 1
        self.last_ping_was_ok = False
        logger.info(
            "Ping failure count run: %s, all (good/bad): %s/%s",
            str(self.ping_failure_count_run),
            str(self.ping_success_count_all),
            str(self.ping_failure_count_all),
        )

        # Set ping status so that the LEDs reflect this change
        self.set_ping_status(False)

        # Only check Wifi status if ping fails
        self.check_wifi_status()

        if root_cfg.my_device.attempt_wifi_recovery:
            self.attempt_wifi_recovery()

    def check_wifi_status(self) -> None:
        # This returns eg "GNX103510:*:95" where * means connected
        essid = utils.run_cmd("nmcli -g SSID,IN-USE,SIGNAL device wifi | grep '*'", ignore_errors=True)
        if len(essid) > 3:
            self.set_wifi_status(True)
            logger.info(f"Wifi is up: {essid}")
        else:
            self.set_wifi_status(False)
            logger.info("Not connected to a wireless access point")

    ##########################################################################################################
    # Attempt to recover WiFi.
    #
    # Possible recovery actions:
    # - Restart the wlan0 interface:  nmcli dev disconnect / connect wlan0
    # - Toggle radio:                 nmcli radio wifi off / on
    # - Explicit wifi connect:        nmcli dev wifi connect <SSID> password <password>
    # - Reload NMCLI:                 nlcli general reload
    # - Restart the device:           sudo reboot
    #
    # Try the first four recovery actions on a 20 minute cycle.
    # These recovery actions are attempted at 2, 4, 6, 8 minutes respectively into each 20 minute cycle.
    # If that doesn't recover it, then after the failure count gets to 4 hours reboot the device.
    ##########################################################################################################
    def attempt_wifi_recovery(self) -> None:
        retry_frequency = 600  # Retry recovery action set every 2s*600=1200s=20mins

        # Ping cycle is 2s, so 60*60*2 = 4 hours.
        if self.ping_failure_count_run == (60 * 60 * 2):
            logger.error(f"{root_cfg.RAISE_WARN()}Rebooting device due to no internet for >4 hours")
            DiagnosticsBundle.collect("Rebooting device due to no internet for >4 hours")
            utils.run_cmd("sudo reboot")

        elif self.ping_failure_count_run % retry_frequency == 60:
            logger.info("Restarting client wifi interface")
            utils.run_cmd("sudo nmcli dev disconnect " + self.client_wlan, ignore_errors=True)
            sleep(1)
            utils.run_cmd("sudo nmcli dev connect " + self.client_wlan, ignore_errors=True)
            sleep(1)

        elif self.ping_failure_count_run % retry_frequency == 120:
            logger.info("Toggle wifi radio")
            utils.run_cmd("sudo nmcli radio wifi off", ignore_errors=True)
            sleep(1)
            utils.run_cmd("sudo nmcli radio wifi on", ignore_errors=True)
            sleep(1)

        elif self.ping_failure_count_run % retry_frequency == 180:
            logger.info("Reloading NetworkManager")
            utils.run_cmd("sudo nmcli general reload", ignore_errors=True)
            sleep(1)

        elif self.ping_failure_count_run % retry_frequency == 240:
            logger.info("Explicitly connecting to wifi network")
            for client in self.wifi_clients:
                if client.ssid is not None and client.pw is not None:
                    logger.info(f"Connecting to {client.ssid}")
                    utils.run_cmd(
                        f"sudo nmcli dev wifi connect {client.ssid} password {client.pw}",
                        ignore_errors=True,
                    )
                    break
            sleep(1)

    def action_on_ping_ok(self) -> None:
        # Track ping stats for logging purposes
        self.ping_success_count_all += 1
        if not self.last_ping_was_ok:
            self.ping_failure_count_run = 0
            self.ping_success_count_run = 0
        self.ping_success_count_run += 1
        if self.ping_success_count_run % 30 == 0:
            logger.info(
                "Ping successful count run: %s, all (good/bad): %s/%s",
                str(self.ping_success_count_run),
                str(self.ping_success_count_all),
                str(self.ping_failure_count_all),
            )

        # Set ping status so that the LEDs reflect this change
        self.set_ping_status(True)

        self.last_ping_was_ok = True

    @staticmethod
    def diagnostics_upload_timer_callback() -> None:
        try:
            logger.debug("Diagnostics upload timer callback")
            DiagnosticsBundle.upload()
        except Exception as e:
            logger.error(
                f"{root_cfg.RAISE_WARN()}Diagnostics upload callback threw an exception: " + str(e),
                exc_info=True,
            )

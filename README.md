# ExPiDITE

ExPiDITE makes it easy to use Raspberry Pi (RPI) for scientific data collection in long-running experiments. ExPiDITE is a pre-baked set of functionality and design choices that reduce the complexity & risk in managing devices, sensors, and data flows.


SENSOR INTEGRATION
- Plug-n-play for a range of common sensors
- Easily extended with new sensors and custom data processing


DATA MANAGEMENT
- Pushes recordings & data directly to cloud storage
- Processes recordings and data on the device or via an ETL for data aggregation and summarisation
- Captures metadata in accordance with FAIR principles


DEVICE MANAGEMENT
- Simplifies management of a "fleet" of RPIs sensors running autonomously
- Provides recipes & functionality for spinning up a secure, internet-accessible dashboard
- Manages upgrade of the RPI OS, the Expidite software and any custom software
- Manages security via a firewall
- Manages Wifi and other network connections
- Controls red/green health status LEDs on the device
- Ensures recording is persistent and reliable over reboots / power cycles / etc


Key design decisions:
- Python: The system is written in Python to make it easy to extend and modify.
- Push-to-cloud: The system pushes data to cloud storage in near-real-time rather than persistently storing it on device.
- Memory-as-disk: The system uses memory-as-disk to reduce wear on the SD card (a key single point of failure).
- Strict file naming: The system enforces strict file naming conventions to ensure that data is easily identifiable and manageable, and related to FAIR records.
- Configuration is stored in Git.


## Installation

To install the code, run:

`pip install git+https://github.com/Oxford-Bee-Ops/expidite`

And follow the instructions in Usage > User Flow below.


## Usage
### PRE-REQUISITES
You will need: 
- a Raspberry Pi SBC and some sensors!
- a GitHub account to store your *fleet* configuration and any custom code you choose to write
- an Azure blobstore account for storage of your sensor output
- some basic experience with Python coding


### USER FLOW - INITIAL SETUP
The following steps enable you to run the default example sensor on your RPI.  Do this first to prove that your cloud storage config is working and to learn the basics.  Then you can move on to defining your actual experimental setup!

- Physically build your RPI and attach your chosen sensors.
- Get an SD card with the Raspberry Pi OS.  If you use Raspberry Pi Imager, enabling SSH access and including default Wifi config will make your life easier.
- Install the SD card and power up your RPI.
- Copy the **keys.env** and **system.cfg** files from the expidite repo `/src/example` folder to your own computer / dev environment / git project.
- Edit **keys.env**:
    - Set `cloud_storage_key` to the Shared Access Signature for your Azure Storage accounts (see explanatory notes in keys.env).
    - For security reasons, do **not** check your keys.env into Git.
- Log in to your RPI:
    - create an **.expidite** folder in your user home directory 
        - `mkdir ~/.expidite`
    - copy your **keys.env** and **system.cfg** to the .expidite folder
    - copy the **rpi_installer.sh** files from `/src/expidite_rpi/scripts` to the .expidite folder
    - run the rpi_installer.sh script:
        - `cd ~/.expidite &&  dos2unix *.sh && chmod +x *.sh && ./rpi_installer.sh`
        - this will take a few minutes as it creates a virtual environment, updates to the latest OS packages, installs Expidite's RpiCore and its dependencies, and sets up the RPI ready for use as a sensor.
    - once RpiCore is installed, you can test it using either:
        - CLI at a shell prompt:
            - `bcli`
            - Option `2. View Status`
        - In Python:
            - `python`
            - `from rpi_core import RpiCore`
            - `rc = RpiCore()`
            - `rc.start()`
- You should see data appearing in each of the containers in your cloud storage account.


### USER FLOW - CONFIGURING FOR YOUR DEVICES
To execute your particular experimental setup, you need to configure your devices in a "fleet config" python file.  You will want to maintain this configuration in Git.

- Create your own Git repo if you haven't already got one
- Copy the `/src/expidite_rpi/example` folder into your Git repo as a starting point for your own config and code customizations.
- Edit **my_fleet_config.py** to add configuration for your device(s)
    - You will need the mac address of the device's wlan0 interface as the identifier of the device
    - To get the mac address run `cat /sys/class/net/wlan0/address`
    - See the example fleet_config.py for more details.
- Edit the **system.cfg**:
    - If you want RpiCore to regularly auto-update your devices to the latest code from your git repo, you will need to set `my_git_repo_url`.
    - See the system.cfg file in `/src/expidite_rpi/example` for more details and more options.

### USER FLOW - PRODUCTION PROCESS FOR AN EXPERIMENT WITH MANY DEVICES
#### Pre-requisites
- You have a **keys.env** with your cloud storage key
- You have a **system.cfg** with:
    - `my_git_repo_url` set to your Git repo URL
    - `auto-start` set to `Yes`
- You have a fleet_config.py file with:
    - all the mac addresses of your devices listed
    - the right sensor configuration for your experiment
    - wifi config set if different from the environment where you're setting them up

#### Deployment
For each device, you will need to:
- Install Raspberry Pi OS on the SD card (or buy it pre-installed)
- Copy on **keys.env**, **system.cfg** and **rpi_installer.sh**
- Install SSH keys so the device can access your private repo - see GitHub.com for details
- Run `./rpi_installer.sh` as per above

With the correct config and auto-start set to yes, your device will immediately start recording - and will continue to do so across reboots / power cycle, etc.

- You can check by running the command line interface (CLI):
    - run `bcli`



### USER FLOW - EXTENDING & CUSTOMIZING
- Supporting new sensors
    - To support new sensors, create a new python file in the same form as my_sensor_example.py that extends **expidite_rpi.Sensor**.
    - You will need to define a configuration object for your sensor that subclasses **expidite_rpi.SensorCfg**.
    - You will need to update your fleet_config to use this new **SensorCfg**.
- Custom processing of recordings or data
    - To implement custom data processing, create a new python file in the same form as my_processor_example.py that extends **expidite_rpi.DataProcessor**.
    - You will need to define a configuration object for your DataProcessor that subclasses **expidite_rpi.DataProcessorCfg**.
    - You will need to update your fleet_config to use this new **DataProcessorCfg**.
- Contributing updates to RpiCore
    - In the first instance, please email admin@bee-ops.com.


### USER FLOW - ETL
- TBD: Setting up an ETL pipeline to process the data

## RPI device management functions
FC=Fleet config; SC=system.cfg; KE=keys.env

| Function  | Config control | Default | Notes |
| ------------- | ------------- | ------------- | ------------- |
| Automatic code updates | FC:`auto_update_code` | Uses crontab + `uv pip install` + your Git project's pyproject.toml to refresh your code and its dependencies (including RpiCore) on a configurable frequency
| Automatic OS updates | FC:`auto_update_os` |  Uses crontab + `sudo apt update && sudo apt upgrade -y` to update the OS on a configurable frequency.  This is a good best practice for staying up to date with security fixes.
| Firewall | SC:`enable_firewall` | Installs and configures UFW (Uncomplicated Firewall)
| Wifi AP awareness | FC:`wifi_clients` | Enable devices to auto-connect to the network by pre-configuring access point details.
| Wifi connections | FC:`attempt_wifi_recovery` | If internet connectivity is lost, try to auto-recover by switching wifi APs and other actions. Requires wifi_clients to be set in the FC.
| Status LEDs | FC:`manage_leds` | Controls a red & green LED used to reflect system status
| SD card wear | SC:`enable_volatile_logs` | Make logging volatile so that it is written to memory rather than the SD card to reduce wear; logs will be lost over reboot as a result but import logs are streamed to cloud storage in real time anyway.

## System setup

| Function  | Config control | Notes |
| ------------- | ------------- | ------------- |
| Cloud storage access key | KE:`cloud_storage_key` | The Shared Access Signature that provides access to your Azure cloud storage
| Auto-start RpiCore | SC:`auto_start` | Starts RpiCore automatically after reboot; unless manual mode invoked via CLI.
| Install a virtual environment | SC:`venv_dir` | Uses uv to install a venv unless one already exists at this location
| Git repo | SC:`my_git_repo_url` | URL of your Git repo containing your configuration and any custom code
| Git branch | SC:`my_git_branch` | Name of the Git branch to use if not main


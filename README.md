# sensor-core

SensorCore makes it as easy to use Raspberry Pi SBCs for scientific data collection
in long-running experiments. SensorCore is a pre-baked set of functionality and design choices that reduce the complexity & risk in managing devices, sensors, and data flows.


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
- Manages upgrade of the RPI OS, the SensorCore software and any custom software
- Manages security via a firewall
- Manages Wifi and other network connections
- Controls red/green health status LEDs on the device
- Ensures recording is persistent and reliable over reboots / power cycles / etc


Key design decisions:
- Python: The system is written in Python to make it easy to extend and modify.
- Push-to-cloud: The system pushes data to cloud storage rather than persistently storing it on device.
- Memory-as-disk: The system uses memory-as-disk to reduce wear on the SD card (a key single point of failure).
- Strict file naming: The system enforces strict file naming conventions to ensure that data is easily identifiable and manageable, and related to FAIR records.
- Configuration is stored in Git.


## Installation

To install the code, run:

`pip install git+https://github.com/Oxford-Bee-Ops/sensor_core`

And follow the instructions in Usage > User Flow below.


## Usage
### PRE-REQUISITES
You will need: 
- a GitHub account to store your *fleet* configuration and any custom code you choose to write
- an Azure account for storage of your sensor output
- a Raspberry Pi SBC and some sensors!
- some basic experience with Python coding


### USER FLOW - INITIAL SETUP
The following steps enable you to run the default example sensor on your RPi.  Do this first to prove that your cloud storage config is working and to learn the basics.  Then you can move on to defining your actual experimental setup!

- In your Azure storage account, create the following containers:
    - `sensor-core-journals` - this is where your experimental data will end up 
    - `sensor-core-system-records` - this is where the SensorCore records will go that show you how your devices are performing
    - `sensor-core-upload` - this is the default location for uploading recordings from your devices
    - `sensor-core-fair` - this is where the FAIR records will go that snapshot your experimental configuration.
- Physically build your Raspberry Pi and attach your chosen sensors.
- Get an SD card with the Raspberry Pi OS.  If you use Raspberry Pi Imager, enabling SSH access and including default Wifi config will make your life easier.
- Install the SD card with and power up your Pi.
- Copy the **keys.env** and **system.cfg** files from the sensor_core repo `/src/example` folder to your own computer / dev environment / git project.
- Edit **keys.env**:
    - Set `cloud_storage_key` to the Shared Access Signature for your Azure Storage accounts (see explanatory notes in keys.env).
    - For security reasons, do **not** check keys.env into Git.
- Log in to your RPi:
    - create a **.sensor_core** folder in your user home directory 
        - `mkdir ~/.sensor_core`
    - copy your **keys.env** and **system.cfg** to the .sensor_core folder
    - copy the **rpi_installer.sh** files from `/src/sensor_core/scripts` to the .sensor_core folder
    - run the rpi_installer.sh script:
        - `cd ~/.sensor_core`
        - `dos2unix *.sh`
        - `chmod +x *.sh`
        - `./rpi_installer.sh`
        - this will take a few minutes as it installs mini-conda, creates a virtual environment, installs SensorCore and its dependencies, and sets up the RPi ready for use as a sensor.
    - once SensorCore is installed, you can test it using either:
        - CLI at a shell prompt:
            - `bcli`
        - In Python:
            - `python`
            - `from sensor_core import SensorCore`
            - `sc = SensorCore()`
            - `sc.start()`
- You should see data appearing in each of the containers in your cloud storage account.


### USER FLOW - CONFIGURING FOR YOUR DEVICES
To execute your particular experimental setup, you need to configure your devices in a "fleet config" python file.  You will want to maintain this configuration in Git.

- Create your own Git repo if you haven't already got one
    - You might want to install `uv` or similar to help you with initial project setup (`uv init`)
- Copy the `/src/example` folder into your Git repo as a starting point for your own config and code customizations.
- Edit **my_fleet_config.py** to add configuration for your device(s)
    - You will need the mac address of the device's wlan0 interface as the identifier of the device
    - To get the mac address run `cat /sys/class/net/wlan0/address`
    - See the example fleet_config.py for more details.
- Edit the **system.cfg**:
    - If you want SensorCore to auto-update your devices each night with the latest code from your git repo, you will need to set `my_git_repo_url`.
    - See the system.cfg file in `/example` for more details and more options.

### USER FLOW - PRODUCTION PROCESS FOR AN EXPERIMENT WITH MANY DEVICES
#### Pre-requisites
- You have a keys.env with your cloud storage key
- You have a system.cfg with:
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
- @@@ Install SSH keys so the device can access your private repo
- Run `./rpi_installer.sh` as per above

With the correct config and auto-start set to yes, your device will immediately start recording - and will continue to do so across reboots / power cycle, etc.


- 
    - create and activate your virtual environment in $HOME/.venv:
        - `python -m venv $HOME/.venv`
        - `source $HOME/venv/bin/activate`
    - install pre-requisites:
        - `sudo apt-get install libsystemd-dev libffi-dev`
        - libsystemd-dev is required by systemd-python to interact with journald
        - libffi-dev is required by azure-storage-blob via cryptography 
        - `sudo apt-get install -y python3-scipy python3-pandas python3-opencv`
    - install sensor-core:
        - `uv pip install git+https://github.com/Oxford-Bee-Ops/sensor_core`
    - install your now-customized example code in **$HOME/code/<my_git_project_name>/**
    - run SensorCore:
        - `cd $HOME/code/<my_git_project_name>`
        - `python run_sensor_core.py`
- If your system.cfg has `auto_start="Yes"`, SensorCore will now be running!
- You can check by running the command line interface (CLI):
    - run `bcli`



### USER FLOW - EXTENDING & CUSTOMIZING
- Supporting new sensors
    - To support new sensors, create a new python file in the same form as my_sensor_example.py that extends **sensor_core.Sensor**.
    - You will need to define a configuration object for your sensor that subclasses **sensor_core.SensorCfg**.
    - You will need to update your fleet_config to use this new SensorCfg.
- Custom processing of recordings or data
    - To implement custom data processing, create a new pythong file in the same form as my_processor_example.py that extends **sensor_core.DataProcessor**.
    - You will need to define a configuration object for your DataProcessor that subclasses **sensor_core.DataProcessorCfg**.
    - You will need to update your fleet_config to use this new DataProcessorCfg.
- Contributing updates to SensorCore
    - In the first instance, please email admin@bee-ops.com.


### USER FLOW - ETL
- Setting up an ETL pipeline to process the data

## RPi device management functions
FC=Fleet config; SC=system.cfg; KE=keys.env

| Function  | Config control | Notes |
| ------------- | ------------- | ------------- |
| Automatic code updates | FC:`auto_update_code` | Uses crontab + `uv pip install` + your Git project's pyproject.toml to refresh your code and its dependencies (including SensorCore) on a configurable frequency
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
| Auto-start SensorCore | SC:`auto_start` | Starts SensorCore automatically after reboot; unless manual mode invoked via CLI.
| Install a virtual environment | SC:`venv_dir` | Uses uv to install a venv unless one already exists at this location
| Git repo | SC:`my_git_repo_url` | URL of your Git repo containing your configuration and any custom code
| Git branch | SC:`my_git_branch` | Name of the Git branch to use if not main


## Class model

- bcli | CLI to SensorCore
- sensor_core.SensorCore is the primary entry point for programmatic integration
    - edge_orchestrator.EdgeOrchestrator manages overall program flow and start/stop of threads
        - dp_node.DPnode superclass implements saving of data to Streams
        - 
        - sensor.Sensor superclass; there will be one per physical sensor
        - dp.DataProcessor superclass; subclasses implement different processing of sensor data 
        - Streams define the data produced by Sensors and DataProcessors
        - dp_node

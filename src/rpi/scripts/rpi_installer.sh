#!/bin/bash

# RPI installer
#
# This script installs / updates SensorCore code according to the ~/.sensor_core/system.cfg file.
# This script is also called from crontab on reboot to restart SensorCore (if auto-start is enabled).
# It is safe to re-run this script multiple times.
#
# Pre-requisites:
# - system.cfg file must exist in the ~/.sensor_core directory
# - keys.env file must exist in the ~/.sensor_core directory
# - SSH enabled on the RPi
# - SSH keys for the user's code repository must exist in the ~/.sensor_core directory
#   if using a private repository
#
# This script will:
# - install_ssh_keys for accessing the user's code repository
# - create_and_activate_venv
# - install_os_packages required for SensorCore
# - install_sensor_core
# - install_user_code
# - install_ufw and configure the firewall rules
# - set_log_storage_volatile so that logs are stored in RAM and don't wear out the SD card
# - create_mount which is a RAM disk for the use by SensorCore
# - set_predictable_network_interface_names
# - enable_i2c
# - alias_bcli so that you can run 'bcli' at any prompt
# - auto_start_if_requested to start SensorCore & DeviceManager automatically if requested in system.cfg
# - make_persistent by adding this script to crontab to run on reboot
#
# This script can be called with a no_os_update argument to skip the OS update and package installation steps.
# This is used when this script is called from crontab on reboot.

# We expect that 1 argument may be passed to this script:
# - no_os_update: if this argument is passed, we skip the OS update and package installation steps
# Check for this argument and set a flag
# to skip the OS update and package installation steps
if [ "$1" == "os_update" ]; then
    os_update="yes"
else
    os_update="no"
fi

# Function to check pre-requisites
check_prerequisites() {
    echo "Checking pre-requisites..."
    if [ ! -d "$HOME/.sensor_core" ]; then
        echo "Error: $HOME/.sensor_core directory is missing"
        exit 1
    fi
    if [ ! -f "$HOME/.sensor_core/system.cfg" ]; then
        echo "Error: system.cfg file is missing in $HOME/.sensor_core"
        exit 1
    fi
    if [ ! -f "$HOME/.sensor_core/keys.env" ]; then
        echo "Error: keys.env file is missing in $HOME/.sensor_core"
        exit 1
    fi
    if ! command -v sudo >/dev/null 2>&1; then
        echo "Error: sudo is not installed or not available"
        exit 1
    fi
    # Check ssh is enabled
    if ! systemctl is-active --quiet ssh; then
        echo "Error: SSH is not enabled. Please enable SSH."
        # This is not a fatal error, but we need to warn the user
    fi
    # Check the OS is 64-bit
    if [ "$(getconf LONG_BIT)" == "64" ] || [ "$(uname -m)" == "aarch64" ]; then
        echo "64-bit OS detected"
    else
        echo "!!! 32-bit OS detected !!!"
        echo "SensorCore is not supported on 32-bit OS because key packages like Ultralytics require 64-bit."
        echo "Please install a 64-bit OS and re-run this script."
        exit 1
    fi
    echo "All pre-requisites are met."

    # Ensure the flags directory exists
    mkdir -p "$HOME/.sensor_core/flags"
    # Delete the reboot_required flag if it exists
    if [ -f "$HOME/.sensor_core/flags/reboot_required" ]; then
        rm "$HOME/.sensor_core/flags/reboot_required"
    fi

}

# Function to get the Git project name from the URL
git_project_name() {
    # Get the Git project name from the URL
    local url="$1"
    local project_name=$(basename "$url" .git)
    echo "$project_name"
}

# Function to read system.cfg file and export the key-value pairs found
export_system_cfg() {
    if [ ! -f "$HOME/.sensor_core/system.cfg" ]; then
        echo "Error: system.cfg file is missing in $HOME/.sensor_core"
        exit 1
    fi
    dos2unix -q "$HOME/.sensor_core/system.cfg" || { echo "Failed to convert system.cfg to Unix format"; exit 1; }
    while IFS='=' read -r key value; do
        if [[ $key != \#* && $key != "" ]]; then
            if [[ $key =~ ^[a-zA-Z_][a-zA-Z0-9_]*$ ]]; then
                # Strip surrounding quotes from the value
                value=$(echo "$value" | sed -e 's/^"//' -e 's/"$//')
                export "$key"="$value"
            else
                echo "Warning: Skipping invalid key '$key' in system.cfg"
            fi
        fi
    done < "$HOME/.sensor_core/system.cfg"
}

# Install SSH keys from the ./sensor_core directory to the ~/.ssh directory
install_ssh_keys() {
    echo "Installing SSH keys..."

    # Skip if the SSH keys already exist
    if [ -f "$HOME/.ssh/$my_git_ssh_private_key_file" ]; then
        echo "SSH keys already installed."
        return
    fi

    # Otherwise, create the ~/.ssh directory if it doesn't exist
    if [ ! -d "$HOME/.ssh" ]; then
        mkdir -p "$HOME/.ssh" || { echo "Failed to create ~/.ssh directory"; exit 1; }
    fi

    # Only install keys if $my_git_ssh_private_key_file is set in the system.cfg file
    if [ -z "$my_git_ssh_private_key_file" ]; then
        echo "my_git_ssh_private_key_file is not set in system.cfg"
    else
        # Copy the users private key file to the ~/.ssh directory
        if [ -f "$HOME/.sensor_core/$my_git_ssh_private_key_file" ]; then
            cp "$HOME/.sensor_core/$my_git_ssh_private_key_file" "$HOME/.ssh/" || { echo "Failed to copy $my_git_ssh_private_key_file to ~/.ssh"; exit 1; }
            chmod 600 "$HOME/.ssh/$my_git_ssh_private_key_file" || { echo "Failed to set permissions for $my_git_ssh_private_key_file"; exit 1; }
        else
            echo "Error: Private key file $my_git_ssh_private_key_file does not exist in $HOME/.sensor_core"
            # This is not a fatal error (it may be intentional if a public repo), but we need to warn the user
        fi

        # Set up known_hosts for GitHub if it doesn't already exist
        if ! ssh-keygen -F github.com > /dev/null; then
            ssh-keyscan github.com >> "$HOME/.ssh/known_hosts"
        fi

        echo "SSH keys installed successfully."
    fi
}

# Function to create a virtual environment if it doesn't already exist
# The venv location is specified in the system.cfg (venv_dir)
create_and_activate_venv() {
    if [ -z "$venv_dir" ]; then
        echo "Error: venv_dir is not set in system.cfg"
        exit 1
    fi

    # Check if the venv directory already exists
    if [ -d "$HOME/$venv_dir" ]; then
        echo "Virtual environment already exists at $HOME/$venv_dir"
    else
        # Create the virtual environment
        echo "Creating virtual environment at $venv_dir..."
        python -m venv "$HOME/$venv_dir" --system-site-packages || { echo "Failed to create virtual environment"; exit 1; }
        echo "Virtual environment created successfully."
    fi

    # Ensure the virtual environment exists before activating
    if [ ! -f "$HOME/$venv_dir/bin/activate" ]; then
        echo "Error: Virtual environment activation script not found"
        exit 1
    fi

    echo "Activating virtual environment..."
    source "$HOME/$venv_dir/bin/activate" || { echo "Failed to activate virtual environment"; exit 1; }
}

# Function to install OS packages using apt-get
# We use this rather than conda or uv because we want packages that are optimised for RPi
# and we want to use the system package manager to install them.
install_os_packages() {
    echo "Installing OS packages..."
    sudo apt-get update && sudo apt-get upgrade -y || { echo "Failed to update package list"; }
    sudo apt-get install -y pip git libsystemd-dev python3-scipy python3-pandas python3-opencv || { echo "Failed to install base packages"; }
    sudo apt-get install -y libcamera-dev python3-picamera2 python3-smbus || { echo "Failed to install sensor packages"; }
    # If we install the lite version (no desktop), we need to install the full version of rpicam-apps
    # Otherwise we get ERROR: *** Unable to find an appropriate H.264 codec ***
    sudo apt-get purge -y rpicam-apps-lite || { echo "Failed to remove rpicam-apps-lite"; }
    sudo apt-get install -y rpi-connect-lite || { echo "Failed to install rpi-connect-lite"; }
    sudo apt-get install -y rpicam-apps || { echo "Failed to install rpicam-apps"; }
    sudo apt-get autoremove -y || { echo "Failed to remove unnecessary packages"; }
    echo "OS packages installed successfully."
    # A reboot is always required after installing packages, otherwise the system is unstable 
    # (eg rpicam broken pipe)
    touch "$HOME/.sensor_core/flags/reboot_required"
}

# Function to install SensorCore 
install_sensor_core() {
    # Install SensorCore from GitHub
    current_version=$(pip show sensor_core | grep Version)
    echo "Installing SensorCore.  Current version: $current_version"
    source "$HOME/$venv_dir/bin/activate" || { echo "Failed to activate virtual environment"; exit 1; }

    ###############################################################################################################
    # We don't return exit code 1 if the install fails, because we want to continue with the rest of the script
    # and this can happen due to transient network issues causing github.com name resolution to fail.
    ###############################################################################################################
    pip install git+https://github.com/oxford-bee-ops/sensor_core.git@main || { echo "Failed to install SensorCore"; }
    updated_version=$(pip show sensor_core | grep Version)
    echo "SensorCore installed successfully.  Now version: $updated_version"

    # If the version has changed, we need to set a flag so we reboot at the end of the script
    if [ "$current_version" != "$updated_version" ]; then
        echo "SensorCore version has changed from $current_version to $updated_version.  Reboot required."
        # Set a flag to indicate that a reboot is required
        touch "$HOME/.sensor_core/flags/reboot_required"
    fi
}

# Function to install user's code
install_user_code() {
    echo "Installing user's code..."

    if [ -z "$my_git_repo_url" ] || [ -z "$my_git_branch" ]; then
        echo "Error: my_git_repo_url or my_git_branch is not set in system.cfg"
        exit 1
    fi

    ############################################
    # Manage SSH prep
    ############################################
    # Verify that the private key file exists
    if [ ! -f "$HOME/.ssh/$my_git_ssh_private_key_file" ]; then
        echo "Error: Private key file ~/.ssh/$my_git_ssh_private_key_file does not exist."
        # This is not a fatal error (it may be intentional if a public repo), but we need to warn the user
    fi

    # Ensure the private key has correct permissions
    chmod 600 "$HOME/.ssh/$my_git_ssh_private_key_file"

    # Set the GIT_SSH_COMMAND with timeout and retry options
    export GIT_SSH_COMMAND="ssh -i $HOME/.ssh/$my_git_ssh_private_key_file -o IdentitiesOnly=yes -o ConnectTimeout=10 -o ConnectionAttempts=5"

    # Persist the GIT_SSH_COMMAND in .bashrc if not already present
    if ! grep -qs "export GIT_SSH_COMMAND=" "$HOME/.bashrc"; then
        echo "export GIT_SSH_COMMAND='ssh -i \$HOME/.ssh/$my_git_ssh_private_key_file -o IdentitiesOnly=yes -o ConnectTimeout=10 -o ConnectionAttempts=5'" >> "$HOME/.bashrc"
    fi

    # Ensure known_hosts exists and add GitHub key if necessary
    mkdir -p "$HOME/.ssh"
    touch "$HOME/.ssh/known_hosts"
    chmod 600 "$HOME/.ssh/known_hosts"
    if ! ssh-keygen -F github.com > /dev/null; then
        echo "Adding GitHub key to known_hosts"
        ssh-keyscan github.com >> "$HOME/.ssh/known_hosts"
    fi

    ##############################################
    # Do the Git clone
    ###############################################
    # [Re-]install the latest version of the user's code in the virtual environment
    # Extract the project name from the URL
    project_name=$(git_project_name "$my_git_repo_url")
    current_version=$(pip show "$project_name" | grep Version)
    echo "Reinstalling user code. Current version: $current_version"
    source "$HOME/$venv_dir/bin/activate" || { echo "Failed to activate virtual environment"; exit 1; }

    ###############################################################################################################
    # We don't return exit code 1 if the install fails, because we want to continue with the rest of the script
    # and this can happen due to transient network issues causing github.com name resolution to fail.
    ###############################################################################################################
    pip install "git+ssh://git@$my_git_repo_url@$my_git_branch" || { echo "Failed to install $my_git_repo_url@$my_git_branch"; }    
    updated_version=$(pip show "$project_name" | grep Version)
    echo "User's code installed successfully. Now version: $updated_version"
    
    # If the version has changed, we need to set a flag so we reboot at the end of the script
    if [ "$current_version" != "$updated_version" ]; then
        echo "User's code version has changed from $current_version to $updated_version.  Reboot required."
        # Set a flag to indicate that a reboot is required
        touch "$HOME/.sensor_core/flags/reboot_required"
    fi
}

# Install the Uncomplicated Firewall and set appropriate rules.
install_ufw() {
    # If enable_firewall="Yes"
    if [ "$enable_firewall" != "Yes" ]; then
        echo "Firewall installation skipped as enable_firewall is not set to 'Yes'."
        return
    fi
    sudo apt-get install -y ufw

    # Clear any current rules
    sudo ufw --force reset

    # Allow IGMP broadcast traffic
    sudo ufw allow proto igmp from any to 224.0.0.1
    sudo ufw allow proto igmp from any to 224.0.0.251
    # Allow SSH on 22 and FTP on 21
    #sudo ufw allow 21
    sudo ufw allow 22
    # Allow DNS on 53
    sudo ufw allow 53
    # Allow DHCP on 67 / 68
    #sudo ufw allow 67/udp
    #sudo ufw allow 68/udp
    # Allow NTP on 123
    #sudo ufw allow 123
    # Allow HTTPS on 443
    sudo ufw allow 443
    # Re-enable the firewall
    sudo ufw --force enable
}

###############################################
# Make log storage volatile to reduce SD card writes
# This is configurable via system.cfg
# Logs then get written to /run/log/journal which is a tmpfs and managed to a maximum size of 50M
###############################################
function set_log_storage_volatile() {
    if [ "$enable_volatile_logs" != "Yes" ]; then
        echo "Skip making storage volatile as enable_volatile_logs is not set to 'Yes'."
        return
    fi
    journal_mode="volatile"
    changes_made=0
    if ! grep -q "Storage=$journal_mode" /etc/systemd/journald.conf; then
        echo "Storage=$journal_mode not set in /etc/systemd/journald.conf; setting it."
        sudo sed -i 's/#Storage=.*/Storage='$journal_mode'/' /etc/systemd/journald.conf
        sudo sed -i 's/Storage=.*/Storage='$journal_mode'/' /etc/systemd/journald.conf
        changes_made=1
    fi
    # Set #SystemMaxUse= to 50M
    journal_size="50M"
    if ! grep -q "SystemMaxUse=$journal_size" "/etc/systemd/journald.conf"; then
        echo "SystemMaxUse=$journal_size not set in /etc/systemd/journald.conf; setting it."
        sudo sed -i 's/#SystemMaxUse=.*/SystemMaxUse='$journal_size'/' /etc/systemd/journald.conf
        sudo sed -i 's/SystemMaxUse=.*/SystemMaxUse='$journal_size'/' /etc/systemd/journald.conf
        changes_made=1
    fi
    # Set #MaxLevelConsole= to debug
    if ! grep -q "MaxLevelConsole=debug" "/etc/systemd/journald.conf"; then
        echo "MaxLevelConsole=debug not set in /etc/systemd/journald.conf; setting it."
        sudo sed -i 's/#MaxLevelConsole=.*/MaxLevelConsole=debug/' /etc/systemd/journald.conf
        sudo sed -i 's/MaxLevelConsole=.*/MaxLevelConsole=debug/' /etc/systemd/journald.conf
        changes_made=1
    fi
    # Restart the journald service if changes were made
    if [ $changes_made -eq 1 ]; then
        echo "Changes made to journald configuration; restarting the service."
        sudo systemctl restart systemd-journald
    fi
}

###############################################
# Create RAM disk
#
# If we're running off an SD card, we use a ramdisk instead of the SD card for the /bee-ops directory.
# If we're running off an SSD, we mount /bee-ops on the SSD.
###############################################
function create_mount() {
    mountpoint="/sensor_core"

    # Create the mount point directory if it doesn't exist
    # We have to do this before we put it in fstab and call sudo mount -a, otherwise it will fail
    if [ ! -d "$mountpoint" ]; then
        echo "Creating $mountpoint"
        sudo mkdir -p $mountpoint
        sudo chown -R $USER:$USER $mountpoint
    fi
    # Are we mounting on SSD or RAM disk?
    if grep -qs "/dev/sda" /etc/mtab; then
        echo "Mounted on SSD; no further action reqd."
    else
        echo "Running on SD card. Mount the RAM disk."
        # All rpi_sensors have a minimum RAM of 4GB, so /dev/shm/ defaults to 2GB
        # We reduce this to 500M for rpi_sensor installations and assign 1.5GB to /bee-ops
        mount_size="1200M"
        if grep -Eqs "$mountpoint.*$mount_size" /etc/fstab; then
            echo "The mount point already exists in fstab with the correct size."
        else
            # If it doesn't exist, we delete any lines that start with "tmpfs /sensor_core" to clean out old config...
            # Such as mounts with the wrong size
            sudo sed -i '/^tmpfs \/sensor_core/d' /etc/fstab

            # ...and then add the new lines
            fstab_entry="tmpfs $mountpoint tmpfs defaults,size=$mount_size,uid=$USER,gid=$USER 0 0"
            echo $fstab_entry

            # Create the mount
            sudo mount -t tmpfs -o size=$mount_size tmpfs $mountpoint

            # Add the mount to fstab
            echo "$fstab_entry" | sudo tee -a /etc/fstab > /dev/null
            sudo systemctl daemon-reload
            # Recommended sleep before mount -a to allow systemd to complete
            sleep 1
            sudo mount -a
            echo "The sensor_core mount point has been added to fstab."
        fi
    fi

}

####################################
# Set predictable network interface names
#
# Runs: sudo raspi-config nonint do_net_names 0
####################################
function set_predictable_network_interface_names() {
    if [ "$enable_predictable_network_interface_names" == "Yes" ]; then
        sudo raspi-config nonint do_net_names 0
        echo "Predictable network interface names set."
    fi
}

####################################
# Enable the I2C interface
#
# Runs:	sudo raspi-config nonint do_i2c 0
####################################
function enable_i2c() {
    if [ "$enable_i2c" == "Yes" ]; then
        sudo raspi-config nonint do_i2c 0
        echo "I2C interface enabled."
    fi
}

##############################################
# Create an alias for the bcli command
##############################################
function alias_bcli() {
    # Create an alias for the bcli command
    if ! grep -qs "alias bcli=" "$HOME/.bashrc"; then
        echo "alias bcli='source ~/$venv_dir/bin/activate && bcli'" >> ~/.bashrc
        echo "Alias for bcli created in .bashrc."
    else
        echo "Alias for bcli already exists in .bashrc."
    fi
    source ~/.bashrc
}

################################################
# Autostart if requested in system.cfg
################################################
function auto_start_if_requested() {
    if [ "$auto_start" == "Yes" ]; then
        echo "Auto-starting SensorCore..."
        
        # Check the script is not already running
        if pgrep -f "$my_start_script" > /dev/null; then
            echo "SensorCore is already running."
            return
        fi
        echo "Calling $my_start_script in $HOME/$venv_dir"
        nohup python -m $my_start_script 2>&1 | /usr/bin/logger -t SENSOR_CORE &
    else
        echo "Auto-start is not enabled in system.cfg."
    fi
}

###############################################
# Make this script persistent by adding it to crontab
# to run on reboot
###############################################
function make_persistent() {
    if [ "$auto_start" == "Yes" ]; then
        # Check the script is in the venv directory
        if [ ! -f "$HOME/$venv_dir/scripts/rpi_installer.sh" ]; then
            echo "Error: rpi_installer.sh not found in $HOME/$venv_dir/scripts/"
            exit 1
        fi
        # Check if the script is already executable
        if [ ! -x "$HOME/$venv_dir/scripts/rpi_installer.sh" ]; then
            chmod +x "$HOME/$venv_dir/scripts/rpi_installer.sh" || { echo "Failed to make rpi_installer.sh executable"; exit 1; }
            echo "rpi_installer.sh made executable."
        fi
        
        rpi_installer_cmd="/bin/bash $HOME/$venv_dir/scripts/rpi_installer.sh 2>&1 | /usr/bin/logger -t SENSOR_CORE"
        rpi_cmd_os_update="/bin/bash $HOME/$venv_dir/scripts/rpi_installer.sh os_update 2>&1 | /usr/bin/logger -t SENSOR_CORE"
        
        # Delete and re-add any lines containing "rpi_installer" from crontab
        crontab -l | grep -v "rpi_installer" | crontab -

        # Add the script to crontab to run on reboot
        echo "Script added to crontab to run on reboot and every night at 2am."
        (crontab -l 2>/dev/null; echo "@reboot $rpi_installer_cmd") | crontab -
        (crontab -l 2>/dev/null; echo "0 2 * * * $rpi_cmd_os_update") | crontab -
    fi
}

################################################
# Reboot if required
################################################
function reboot_if_required() {
    if [ -f "$HOME/.sensor_core/flags/reboot_required" ]; then
        echo "Reboot required. Rebooting now..."
        sudo reboot
    else
        echo "No reboot required."
    fi
}

###################################################################################################
#
# Main script execution to configure a RPi device suitable for long-running SensorCore operations
# 
###################################################################################################
echo "Starting RPi installer.  (os_update=$os_update)"
# Sleep for 20 seconds to allow the system to settle down after booting
sleep 10
check_prerequisites
cd "$HOME/.sensor_core" || { echo "Failed to change directory to $HOME/.sensor_core"; exit 1; }
export_system_cfg
install_ssh_keys
create_and_activate_venv
# If the os_update argument is passed on the command line, update the OS
if [ "$os_update" == "yes" ]; then
    install_os_packages
fi
install_sensor_core
install_user_code
install_ufw
set_log_storage_volatile
create_mount
set_predictable_network_interface_names
enable_i2c
alias_bcli
auto_start_if_requested
make_persistent
reboot_if_required

# Add a flag file in the .sensor_core directory to indicate that the installer has run
# We use the timestamp on this flag to determine the last update time
touch "$HOME/.sensor_core/flags/rpi_installer_ran"
echo "RPi installer completed successfully."


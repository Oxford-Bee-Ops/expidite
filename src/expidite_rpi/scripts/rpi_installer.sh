#!/bin/bash

# RPI installer
#
# This script installs / updates RpiCore code according to the ~/.expidite/system.cfg file.
# This script is also called from crontab on reboot to restart RpiCore (if auto-start is enabled).
# It is safe to re-run this script multiple times.
#
# Pre-requisites:
# - system.cfg file must exist in the ~/.expidite directory
# - keys.env file must exist in the ~/.expidite directory
# - SSH enabled on the RPi
# - SSH keys for the user's code repository must exist in the ~/.expidite directory
#   if using a private repository
#
# This script will:
# - install_ssh_keys for accessing the user's code repository
# - create_and_activate_venv
# - install_os_packages required for RpiCore
# - install_expidite
# - install_user_code
# - install_ufw and configure the firewall rules
# - set_log_storage_volatile so that logs are stored in RAM and don't wear out the SD card
# - create_mount which is a RAM disk for the use by RpiCore
# - set_predictable_network_interface_names
# - enable_i2c
# - alias_bcli so that you can run 'bcli' at any prompt
# - auto_start_if_requested to start RpiCore & DeviceManager automatically if requested in system.cfg
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
    if [ ! -d "$HOME/.expidite" ]; then
        echo "Error: $HOME/.expidite directory is missing"
        exit 1
    fi
    if [ ! -f "$HOME/.expidite/system.cfg" ]; then
        echo "Error: system.cfg file is missing in $HOME/.expidite"
        exit 1
    fi
    if [ ! -f "$HOME/.expidite/keys.env" ]; then
        echo "Error: keys.env file is missing in $HOME/.expidite"
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
        echo "RpiCore is not supported on 32-bit OS because key packages like Ultralytics require 64-bit."
        echo "Please install a 64-bit OS and re-run this script."
        exit 1
    fi
    echo "All pre-requisites are met."

    # Ensure the flags directory exists
    mkdir -p "$HOME/.expidite/flags"
    # Delete the reboot_required flag if it exists
    if [ -f "$HOME/.expidite/flags/reboot_required" ]; then
        rm "$HOME/.expidite/flags/reboot_required"
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
    if [ ! -f "$HOME/.expidite/system.cfg" ]; then
        echo "Error: system.cfg file is missing in $HOME/.expidite"
        exit 1
    fi
    dos2unix -q "$HOME/.expidite/system.cfg" || { echo "Failed to convert system.cfg to Unix format"; exit 1; }
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

    # If expidite_git_branch is not set, default to main
    if [ -z "$expidite_git_branch" ]; then
        expidite_git_branch="main"
    fi

    done < "$HOME/.expidite/system.cfg"
}

# Install SSH keys from the ./expidite directory to the ~/.ssh directory
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
        if [ -f "$HOME/.expidite/$my_git_ssh_private_key_file" ]; then
            cp "$HOME/.expidite/$my_git_ssh_private_key_file" "$HOME/.ssh/" || { echo "Failed to copy $my_git_ssh_private_key_file to ~/.ssh"; exit 1; }
            chmod 600 "$HOME/.ssh/$my_git_ssh_private_key_file" || { echo "Failed to set permissions for $my_git_ssh_private_key_file"; exit 1; }
        else
            echo "Error: Private key file $my_git_ssh_private_key_file does not exist in $HOME/.expidite"
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

        # Export a variable to indicate that this was a new install and that an os update is required
        export new_install="yes"
        export os_update="yes"
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
    sudo apt-get install -y pip git libsystemd-dev ffmpeg python3-scipy python3-pandas python3-opencv || { echo "Failed to install base packages"; }
    sudo apt-get install -y libcamera-dev python3-picamera2 python3-smbus || { echo "Failed to install sensor packages"; }
    # If we install the lite version (no desktop), we need to install the full version of rpicam-apps
    # Otherwise we get ERROR: *** Unable to find an appropriate H.264 codec ***
    sudo apt-get purge -y rpicam-apps-lite || { echo "Failed to remove rpicam-apps-lite"; }
    sudo apt-get install -y rpi-connect-lite rpicam-apps || { echo "Failed to install rpi connect and rpicam-apps"; }
    sudo apt-get autoremove -y || { echo "Failed to remove unnecessary packages"; }
    echo "OS packages installed successfully."
    # A reboot is always required after installing packages, otherwise the system is unstable 
    # (eg rpicam broken pipe)
    touch "$HOME/.expidite/flags/reboot_required"
}

# Function to install Expidite's RpiCore 
install_expidite() {
    # Install expidite from GitHub
    current_version=$(pip show expidite | grep Version)
    echo "Installing expidite.  Current version: $current_version"
    source "$HOME/$venv_dir/bin/activate" || { echo "Failed to activate virtual environment"; exit 1; }

    ###############################################################################################################
    # We don't return exit code 1 if the install fails, because we want to continue with the rest of the script
    # and this can happen due to transient network issues causing github.com name resolution to fail.
    ###############################################################################################################
    # Check if the branch exists
    if ! git ls-remote --heads https://github.com/oxford-bee-ops/expidite.git "$expidite_git_branch" > /dev/null; then
        echo "Warning: Branch '$expidite_git_branch' does not exist in the repository."
    fi

    pip install "git+https://github.com/oxford-bee-ops/expidite.git@$expidite_git_branch" || { echo "Failed to install Expidite"; }
    updated_version=$(pip show expidite | grep Version)
    echo "Expidite installed successfully.  Now version: $updated_version"

    # We store the updated_version in the flags directory for later use in logging
    echo "$updated_version" > "$HOME/.expidite/expidite_code_version"

    # If the version has changed, we need to set a flag so we reboot at the end of the script
    if [ "$current_version" != "$updated_version" ]; then
        echo "Expidite version has changed from $current_version to $updated_version.  Reboot required."
        # Set a flag to indicate that a reboot is required
        touch "$HOME/.expidite/flags/reboot_required"
    fi
}

fix_my_git_repo() {
    # Fix the Git repository URL if we're doing a direct git clone for system test installations
    # Normal URL format: git@github.com/oxford-bee-ops/expidite.git
    # SSH URL format:    git@github.com:oxford-bee-ops/expidite.git
    #
    # Replace the slash following github.com with a colon
    # This is required for the git clone command to work with SSH
    # The colon is required for SSH URLs, but not for HTTPS URLs
    git_repo_url="$1"
    if [[ $git_repo_url == *"github.com/"* ]]; then
        # Replace the slash with a colon
        git_repo_url=${git_repo_url/github.com/github.com:}
    fi
    echo "$git_repo_url"
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
    export GIT_SSH_COMMAND="ssh -i $HOME/.ssh/$my_git_ssh_private_key_file -o IdentitiesOnly=yes -o ConnectTimeout=10 -o ConnectionAttempts=2"

    # Persist the GIT_SSH_COMMAND in .bashrc if not already present
    if ! grep -qs "export GIT_SSH_COMMAND=" "$HOME/.bashrc"; then
        echo "export GIT_SSH_COMMAND='ssh -i \$HOME/.ssh/$my_git_ssh_private_key_file -o IdentitiesOnly=yes -o ConnectTimeout=10 -o ConnectionAttempts=2'" >> "$HOME/.bashrc"
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
    ##############################################
    # [Re-]install the latest version of the user's code in the virtual environment
    # Extract the project name from the URL
    project_name=$(git_project_name "$my_git_repo_url")
    current_version=$(pip show "$project_name" | grep Version)
    echo "Reinstalling user code. Current version: $current_version"
    source "$HOME/$venv_dir/bin/activate" || { echo "Failed to activate virtual environment"; exit 1; }

    # 1. Get remote HEAD commit hash
    REMOTE_HASH=$(git ls-remote "git@$my_git_repo_url" "refs/heads/$my_git_branch" | awk '{print $1}')

    # 2. Load last-installed hash (if any)
    HASH_FILE="$HOME/.expidite/flags/user-repo-last-hash"
    if [[ -f "$HASH_FILE" ]]; then
        LOCAL_HASH=$(<"$HASH_FILE")
    else
        LOCAL_HASH=""
    fi

    # 3. Compare and install only if changed
    if [[ "$REMOTE_HASH" != "$LOCAL_HASH" ]]; then
        echo "Detected new commit $REMOTE_HASH on branch $my_git_branch."

        # We don't return exit code 1 if the install fails, because we want to continue with the rest of the script
        # and this can happen due to transient network issues causing github.com name resolution to fail.
        if [ "$install_type" == "system_test" ]; then
            # On system test installations, we want the test code as well, so we run pip install .[dev]
            project_dir="$HOME/$venv_dir/src/$project_name"
            if [ -d "$project_dir" ]; then
                # Delete the .git directory to avoid issues with git clone
                rm -rf "$project_dir"
            fi
            mkdir -p "$project_dir"
            cd "$project_dir"
            my_git_repo_url=$(fix_my_git_repo "$my_git_repo_url")
            git clone --depth 1 --branch "$my_git_branch" "git@${my_git_repo_url}" "$project_dir"
            pip install .[dev] || { echo "Failed to install system test code"; }
            cd "$HOME/.expidite" 
        else
            pip install "git+ssh://git@$my_git_repo_url@$my_git_branch" || { echo "Failed to install $my_git_repo_url@$my_git_branch"; }    
        fi

        # Cache the new hash
        echo "$REMOTE_HASH" > "$HASH_FILE"
        echo "Installation complete; hash updated."
    else
        echo "No changes on $BRANCH ($REMOTE_HASH). Skipping install."
    fi
    
    updated_version=$(pip show "$project_name" | grep Version)
    echo "User's code installed successfully. Now version: $updated_version"

    # We store the updated_version in the flags directory for later use in logging
    echo "$updated_version" > "$HOME/.expidite/user_code_version"

    # If the version has changed, we need to set a flag so we reboot at the end of the script
    if [ "$current_version" != "$updated_version" ]; then
        echo "User's code version has changed from $current_version to $updated_version.  Reboot required."
        # Set a flag to indicate that a reboot is required
        touch "$HOME/.expidite/flags/reboot_required"
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
    sudo ufw allow 67/udp
    sudo ufw allow 68/udp
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
    mountpoint="/expidite"

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
            # If it doesn't exist, we delete any lines that start with "tmpfs /expidite" to clean out old config...
            # Such as mounts with the wrong size
            sudo sed -i '/^tmpfs \/expidite/d' /etc/fstab

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
            echo "The expidite mount point has been added to fstab."
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
# Set hostname to "expidite-<device_id>"
# The device_id is the wlan0 mac address with the colons removed
##############################################
function set_hostname() {
    if [ ! -f /sys/class/net/wlan0/address ]; then
        echo "Error: wlan0 interface not found."
        return 1
    fi 
    mac_address=$(cat /sys/class/net/wlan0/address)
    device_id=$(echo "$mac_address" | tr -d ':')

    # Set the hostname to expidite-<device_id>
    new_hostname="expidite-$device_id"
    
    # Check if the hostname is already set correctly
    current_hostname=$(hostname)
    if [ "$current_hostname" == "$new_hostname" ]; then
        echo "Hostname is already set to $new_hostname."
        return 0
    fi

    # Set the new hostname
    echo "Setting hostname to $new_hostname..."
    sudo hostnamectl set-hostname "$new_hostname" || { echo "Failed to set hostname"; exit 1; }
    
    # Update /etc/hosts file
    if ! grep -q "$new_hostname" /etc/hosts; then
        echo "Updating /etc/hosts file..."
        # Remove any line starting with "127.0.1.1"
        sudo sed -i '/^127\.0\.1\.1/d' /etc/hosts
        # Insert the new hostname at the end of the file
        echo "127.0.1.1 $new_hostname" | sudo tee -a /etc/hosts > /dev/null
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
    # We make this conditional on both auto_start and this not being a system_test install
    if [ "$auto_start" == "Yes" ] && [ "$install_type" != "system_test" ]; then
        echo "Auto-starting Expidite RpiCore..."
        
        # Check the script is not already running
        if pgrep -f "$my_start_script" > /dev/null; then
            echo "Expidite RpiCore is already running."
            return
        fi
        echo "Calling $my_start_script in $HOME/$venv_dir"
        nohup python -m $my_start_script 2>&1 | /usr/bin/logger -t EXPIDITE &
    else
        echo "Auto-start is not enabled in system.cfg or not appropriate to this install type."
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
        
        rpi_installer_cmd="/bin/bash $HOME/$venv_dir/scripts/rpi_installer.sh 2>&1 | /usr/bin/logger -t EXPIDITE"
        rpi_cmd_os_update="/bin/bash $HOME/$venv_dir/scripts/rpi_installer.sh os_update 2>&1 | /usr/bin/logger -t EXPIDITE"
        
        # Delete and re-add any lines containing "rpi_installer" from crontab
        crontab -l | grep -v "rpi_installer" | crontab -

        # Add the script to crontab to run on reboot
        echo "Script added to crontab to run on reboot and Saturday night at 2am."
        (crontab -l 2>/dev/null; echo "@reboot $rpi_installer_cmd") | crontab -
        (crontab -l 2>/dev/null; echo "0 2 * * 6 $rpi_cmd_os_update") | crontab -
    fi

    # If this is a system_test install, we add an additional line to crontab to run my_start_script
    # every night at 4am
    if [ "$install_type" == "system_test" ]; then
        # Delete and re-add any lines containing $my_start_script from crontab
        crontab -l | grep -v "$my_start_script" | crontab -        
        echo "Script added to crontab to run $my_start_script every night at 4am."
        # Activate the virtual environment and run the script
        # We use nohup to run the script in the background and redirect output to logger
        (crontab -l 2>/dev/null; \
        echo "0 4 * * * $HOME/$venv_dir/bin/python -m $my_start_script 2>&1 | /usr/bin/logger -t EXPIDITE") | crontab -
    fi
}

################################################
# Reboot if required
################################################
function reboot_if_required() {
    if [ -f "$HOME/.expidite/flags/reboot_required" ]; then
        echo "Reboot required. Rebooting now..."
        sudo reboot
    else
        echo "No reboot required."
    fi
}

###################################################################################################
#
# Main script execution to configure a RPi device suitable for long-running RpiCore operations
# 
###################################################################################################
echo "Starting RPi installer.  (os_update=$os_update)"
# On reboot, os_update is set to "no".
# Sleep for 10 seconds to allow the system to settle down after booting
sleep 10
check_prerequisites
cd "$HOME/.expidite" || { echo "Failed to change directory to $HOME/.expidite"; exit 1; }
export_system_cfg
install_ssh_keys
create_and_activate_venv
if [ "$os_update" == "yes" ]; then
    # OS updates occur on a cron job which could lead to all devices being updated at the same time.
    # To avoid overloading the network, we stagger updates by a random amount between 0 and 20 minutes.
    # The system will still be running happily while this script sleeps.
    # A manual new install will not sleep.
    if [ "$new_install" != "yes" ]; then
        sleep_time=$((RANDOM % 1200)) 
        echo "Sleeping for $sleep_time seconds to stagger updates."
        sleep $sleep_time
    fi
    install_os_packages
fi
install_expidite
install_user_code
install_ufw
set_log_storage_volatile
create_mount
set_predictable_network_interface_names
enable_i2c
alias_bcli
set_hostname
auto_start_if_requested
make_persistent
reboot_if_required

# Add a flag file in the .expidite directory to indicate that the installer has run
# We use the timestamp on this flag to determine the last update time
touch "$HOME/.expidite/flags/rpi_installer_ran"
echo "Expidite RPi installer completed successfully."


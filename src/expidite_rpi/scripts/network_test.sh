#!/bin/bash
##########################################################################################
# Script to test the wifi strength and associated network setup / performance
##########################################################################################
device_type="rpi_sensor"
mode="mode5"
source /home/bee-ops/code/bee_ops_code/common/sensor_utils.sh

# We want to write the output of this script to both screen (stdout) and to a log file
# Redirect stdout ( > ) into a named pipe ( >() ) running "tee"
exec > >(tee -i $(get_tmp_file_name "network_test_output.log"))
exec 2>&1

# Check if the parameter q has been passed in
if [ "$1" == "q" ]; then
    echo "Running in quick mode"
    quickmode=true
else
    echo "Running in full mode"
    quickmode=false
fi

# Dump connection config info using nmcli
echo "Dumping connection config info using nmcli"
sudo nmcli connection show

# Dump the wifi configuration
echo -e "\nDumping the wifi configuration and get info on wlan0 and wlan1"
sudo cat /etc/wpa_supplicant/wpa_supplicant.conf
sudo iw wlan0 info
#sudo iw wlan1 info

# Dump info on the strength of the wifi signal
echo -e "\nDumping info on the strength of the wifi signal"
sudo iwconfig

# Dump info on how many devices are attached to our AP
echo -e "\nDumping info on how many devices are attached to our AP"
sudo iw dev wlan0 station dump

# Dump the DNS configuration
echo -e "\nDumping the DNS configuration"
sudo cat /etc/resolv.conf

# Dumping arp info
echo -e "\nDumping arp info"
arp -n

# Dumping ifconfig
echo -e "\nDumping ifconfig"
ifconfig

# Run traceroute to a URL
# if traceroute is not installed, install it with sudo apt-get install traceroute
echo -e "\nRunning traceroute to a URL"
if ! [ -x "$(command -v traceroute)" ]; then
  sudo apt-get install traceroute
fi
traceroute www.google.com

# Skip long-running tests in quick mode
if [ "$quickmode" = false ] ; then
    # Dump the wifi cell info
    echo -e "\nDumping the wifi cell info"
    sudo nmcli device wifi list

    # Run a ping test to a fixed IP address for 20 seconds to get packet loss info
    echo -e "\nRunning 20 ping tests to a fixed IP address to get packet loss info"
    ping -c 20 8.8.8.8

    # Loop 20 times running a single ping test to a URL to look for DNS issues
    # Use a URL other than google.com to avoid DNS caching
    echo -e "\nLooping 20 times running a single ping test to amazon.com URL to look for DNS issues"
    for i in {1..20}
    do
        # We suppress the output of ping unless it fails
        ping -c 1 www.amazon.com > /dev/null || echo "Ping to www.amazon.com failed on attempt $i"
        sleep 0.3
    done

    # Run a speed test
    # if speedtest-cli is not installed, install it with sudo apt-get install speedtest-cli
    echo -e "\nRunning a speed test"
    if ! [ -x "$(command -v speedtest-cli)" ]; then
    sudo apt-get install speedtest-cli
    fi
    speedtest-cli
fi
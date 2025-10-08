#!/bin/bash
##########################################################################################
# Script to test the wifi strength and associated network setup / performance
##########################################################################################
device_type="rpi_sensor"
mode="mode5"

# Check if the parameter q has been passed in
if [ "$1" == "q" ]; then
    echo "Running in quick mode"
    quickmode=true
else
    echo "Running in full mode"
    quickmode=false
fi

# Dump connection config info using nmcli
echo -e "\nDumping connection config info"
sudo nmcli connection show
echo -e "\nDumping wifi signal info"
sudo nmcli device wifi list

# Dump the wifi configuration
echo -e "\nDumping iwconfig"
sudo iwconfig

# Dumping ifconfig
echo -e "\nDumping ifconfig"
ifconfig

# Dump the DNS configuration
echo -e "\nDumping the DNS configuration"
sudo cat /etc/resolv.conf

# Dumping arp info
echo -e "\nDumping arp info"
arp -n

# Run traceroute to a URL
# if traceroute is not installed, install it with sudo apt-get install traceroute
echo -e "\nPing www.google.com"
ping -c 3 www.google.com

# Skip long-running tests in quick mode
if [ "$quickmode" = false ] ; then

    # Run a speed test
    # if speedtest-cli is not installed, install it with sudo apt-get install speedtest-cli
    echo -e "\nRunning a speed test"
    if ! [ -x "$(command -v speedtest-cli)" ]; then
    sudo apt-get install speedtest-cli
    fi
    speedtest-cli
fi
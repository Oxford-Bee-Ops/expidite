from __future__ import annotations

import sys

import click

from expidite_rpi.core import configuration as root_cfg
from expidite_rpi.core.device_config_objects import DeviceCfg

logger = root_cfg.setup_logger("expidite")

DASH_LINE = "#" * 120


def check_keys_env() -> bool:
    """Check if the keys.env exists in ./rpi_core and is not empty."""
    success, error = root_cfg.check_keys()
    if success:
        return True

    click.echo(f"{DASH_LINE}")
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
    click.echo(f"{DASH_LINE}")
    return False


def check_device_in_inventory() -> None:
    """Check if this device's ID is found in the fleet configuration inventory."""
    try:
        if root_cfg.my_device_id not in root_cfg.INVENTORY:
            click.echo(f"{DASH_LINE}")
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
            click.echo(f"{DASH_LINE}")
    except Exception as e:
        logger.debug(f"Error checking device inventory: {e}")


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

    check_device_in_inventory()


def load_and_set_inventory() -> list[DeviceCfg] | None:
    """Load fleet configuration and set the global inventory.

    Returns the inventory list, or None if loading failed.
    """
    inventory = root_cfg.load_configuration()
    if inventory:
        root_cfg.set_inventory(inventory)
    return inventory

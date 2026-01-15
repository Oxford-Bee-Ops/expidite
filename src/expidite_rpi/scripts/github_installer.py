#!/usr/bin/env python3
##############################################################################################################
# Purpose: download and install the latest version of the ML models package from GitHub.
##############################################################################################################

# TODO - Call this script from rpi_installer.sh - write install_user_code_from_package() function.

# TODO Load config or pass parameters (need GitHub PAT, my_git_repo, current_version, my_git_branch).
# TODO: Init GitHub Auth object.
# TODO Get list of releases from GitHub API (no filter).
# TODO Filter only for current branch.
# TODO Identify latest release (highest version number) for that branch.
# TODO If already installed, log and exit.
# TODO Log and download the wheel.
# TODO Log and pip install the wheel.
# TODO MUST FAIL THE INSTALL OVERALL IF THIS STEP FAILS.
# TODO If successful, log and set reboot_required (or defer to rpi_installer.sh).
# TODO If successful, write new current_version file (or defer to rpi_installer.sh).

# --- end ---


#!/usr/bin/env python3

from importlib.metadata import PackageNotFoundError, version

from github import Auth, Github, GithubException
from packaging.version import Version

from expidite_rpi import configuration as root_cfg

##############################################################################################################
# Purpose: download and install the latest version of the bee_ops package from GitHub for the configured
# branch.
#############################################################################################################


def _get_my_github_pat() -> str:
    if (
        root_cfg.keys is None
        or root_cfg.keys.my_git_pat is None
        or root_cfg.keys.my_git_pat == root_cfg.FAILED_TO_LOAD
        or len(root_cfg.keys.my_git_pat) == 0
        # or not isinstance(root_cfg.keys.my_git_pat, str)
    ):
        raise ValueError("GitHub PAT missing from keys.env")
    assert isinstance(root_cfg.keys.my_git_pat, str)
    return root_cfg.keys.my_git_pat


def _get_repo_path() -> str:
    """Extract organization/repo from a GitHub URL."""
    parts = root_cfg.system_cfg.my_git_repo_url.removesuffix(".git").rstrip("/").split("/")
    return f"{parts[-2]}/{parts[-1]}"


def _get_my_git_banch() -> str:
    return root_cfg.system_cfg.my_git_branch


def _get_installed_bee_ops_version() -> str:
    """Get version of an installed package."""
    try:
        return version("bee_ops")
    except PackageNotFoundError:
        return "0.0.0"


def _get_latest_bee_ops_version() -> str:
    g = Github(auth=Auth.Token(_get_my_github_pat()))
    repo = g.get_repo(_get_repo_path())
    releases = repo.get_releases()
    if releases is None or releases.totalCount == 0:
        raise RuntimeError("No releases found")

    # Only interested in releases for the configured branch.
    my_git_branch = _get_my_git_banch()
    latest_version_found = "0.0.0"

    for release in releases:
        release_branch = release.tag_name.split("-v")[0]
        if release_branch == my_git_branch:
            release_version = release.tag_name.split("-v")[1]
            if Version(release_version) > Version(latest_version_found):
                latest_version_found = release_version

    return latest_version_found


def _download_and_install_package(latest_version: str) -> None:
    pass  # TODO


def _install_bee_ops_package() -> None:
    """Download and install the latest version of the bee_ops package from GitHub."""
    installed_version = _get_installed_bee_ops_version()

    try:
        latest_version = _get_latest_bee_ops_version()
        print(f"bee_ops package: installed: {installed_version}, latest: {latest_version}")

        if installed_version == latest_version:
            print("Latest version already installed. No action needed.")
            return

            _download_and_install_package(latest_version)
    except GithubException as e:
        print(f"Failed to read bee_ops repo: {e}")
        raise


if __name__ == "__main__":
    print("Installing bee_ops package...")

    try:
        _install_bee_ops_package()
        print("Installation of bee_ops package complete")
    except Exception as e:
        print(f"Installation of bee_ops package failed: {e}")
        # TODO need a return code instead.

# TODO Log and download the wheel.
# TODO Log and pip install the wheel.
# TODO MUST FAIL THE INSTALL OVERALL IF THIS STEP FAILS.
# TODO If successful, log and set reboot_required (or defer to rpi_installer.sh).
# TODO If successful, write new current_version file (or defer to rpi_installer.sh).
# TODO Fix all TODOs in rpi_installer.sh.

# --- end ---

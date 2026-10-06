#!/usr/bin/env python3

import importlib.resources
import subprocess
import sys
import tempfile
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

from github import Auth, Github, GithubException
from github.GitRelease import GitRelease
from packaging.utils import canonicalize_name
from packaging.version import Version

from expidite_rpi import configuration as root_cfg

##############################################################################################################
# Purpose: download and install the latest version of the user repo package from GitHub for the configured
# branch.
##############################################################################################################


def _get_my_github_pat() -> str:
    if (
        root_cfg.keys is None
        or root_cfg.keys.my_git_pat is None
        or root_cfg.keys.my_git_pat == root_cfg.FAILED_TO_LOAD
        or len(root_cfg.keys.my_git_pat) == 0
        # or not isinstance(root_cfg.keys.my_git_pat, str)
    ):
        msg = "GitHub PAT missing from keys.env"
        raise ValueError(msg)
    assert isinstance(root_cfg.keys.my_git_pat, str)
    return root_cfg.keys.my_git_pat


def _get_repo_path() -> str:
    """Extract organization/repo from a GitHub URL."""
    parts = root_cfg.system_cfg.my_git_repo_url.removesuffix(".git").rstrip("/").split("/")
    return f"{parts[-2]}/{parts[-1]}"


def _get_my_git_banch() -> str:
    return root_cfg.system_cfg.my_git_branch


def _get_my_package_name() -> str:
    return root_cfg.system_cfg.my_package_name


def _get_installed_user_repo_version() -> str:
    """Get version of the user repo package."""
    try:
        return version(_get_my_package_name())
    except PackageNotFoundError:
        return "0.0.0"


def _get_latest_user_repo_version(g: Github) -> tuple[str, GitRelease | None]:
    repo = g.get_repo(_get_repo_path())
    releases = repo.get_releases()
    if releases is None or releases.totalCount == 0:
        msg = "No releases found"
        raise RuntimeError(msg)

    # Only interested in releases for the configured branch.
    my_git_branch = _get_my_git_banch()
    latest_version_found = "0.0.0"
    latest_release_found = None

    for release in releases:
        release_parts = release.tag_name.rsplit("-", 1)
        release_branch = release_parts[0]
        if release_branch == my_git_branch:
            release_version = release_parts[1]
            if Version(release_version) > Version(latest_version_found):
                latest_version_found = release_version
                latest_release_found = release

    return latest_version_found, latest_release_found


def _download_and_install_package(release: GitRelease) -> None:
    if release.assets is not None:
        for asset in release.assets:
            if asset.name.endswith(".whl"):
                print(f"Downloading: {asset.name}")
                with tempfile.TemporaryDirectory() as temp_dir:
                    local_wheel_path = Path(temp_dir) / asset.name
                    asset.download_asset(str(local_wheel_path))
                    _install_package(local_wheel_path)
                    return

    msg = "No user package found"
    raise AssertionError(msg)


def _install_package(local_wheel_path: Path) -> None:
    try:
        # rpi_installer.sh pipes our stdout through tee, so it is block-buffered. Flush before the child
        # writes to the same pipe, or its output lands in the log ahead of lines we printed earlier.
        sys.stdout.flush()
        subprocess.check_call(
            [
                sys.executable,
                "-m",
                "pip",
                "install",
                "--no-cache-dir",
                "--upgrade",
                "--upgrade-strategy",
                "only-if-needed",
                str(local_wheel_path),
            ]
        )
        print(f"Successfully installed {local_wheel_path.name}")
    except subprocess.CalledProcessError as e:
        print(f"Failed to install {local_wheel_path.name}: {e}")
        raise


def _run_package_post_install(package_name: str) -> None:
    """If the user package includes a post-install script, run it now.

    This runs on every installer pass, not only when a new version of the user package was installed, so the
    script must be idempotent. Running it only on a version change would make a failed post-install permanent:
    the package itself had already installed, so every later pass saw the latest version present and skipped
    the script, leaving whatever it was meant to set up missing until someone fixed the device by hand.
    """
    try:
        scripts_module = importlib.import_module(f"{package_name}.scripts")
        post_install_path = importlib.resources.files(scripts_module) / "post-install.sh"

        # Flush so the script's output doesn't overtake ours; see _install_package.
        sys.stdout.flush()
        subprocess.check_call(["bash", str(post_install_path)])
        print(f"Successfully ran post-install script for {package_name}")
    except (ModuleNotFoundError, FileNotFoundError):
        # The post-install.sh script is optional, so this is fine.
        pass
    except subprocess.CalledProcessError as e:
        print(f"Post-install script failed for {package_name}: {e}")
        raise


def _install_user_repo_package() -> None:
    """Download and install the latest version of the user repo package from GitHub."""
    installed_version = _get_installed_user_repo_version()

    try:
        g = Github(auth=Auth.Token(_get_my_github_pat()))

        latest_version, latest_release = _get_latest_user_repo_version(g)
        print(f"User package: installed: {installed_version}, latest: {latest_version}")

        if installed_version == latest_version:
            print("Latest version already installed.")
        else:
            assert latest_release is not None
            _download_and_install_package(latest_release)

        # Match wheel name normalization: lowercase and collapse runs of dots, hyphens and underscores.
        package_name = canonicalize_name(_get_my_package_name()).replace("-", "_")
        _run_package_post_install(package_name)
    except GithubException as e:
        print(f"Failed to read user package: {e}")
        raise


if __name__ == "__main__":
    try:
        _install_user_repo_package()
        print("Installation of user package complete")
    except Exception as e:
        print(f"Installation of user package failed: {e}")
        # We don't return any indication that the installation failed because we want the caller to continue
        # with the rest of the script and failures can happen due to transient network issues causing
        # github.com name resolution to fail.

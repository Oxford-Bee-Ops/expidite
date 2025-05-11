import os
from pathlib import Path
from typing import Optional

import git

from sensor_core import configuration as root_cfg

logger = root_cfg.setup_logger("sensor_core")

def initialize_git_repo(git_url: str, 
                        git_branch: str = "main", 
                        ssh_key_path: Optional[Path | str] = None) -> None:
    """Initialize a Git repository by cloning it if it doesn't exist."""
    
    if not git_url or git_url == root_cfg.FAILED_TO_LOAD:
        logger.error(f"{root_cfg.RAISE_WARN()}my_git_repo_url is not defined in system configuration.")
        return
    
    if git_branch == root_cfg.FAILED_TO_LOAD:
        git_branch = "main"

    # Get the repo name from the URL
    repo_path = _get_repo_path(git_url)

    if os.path.exists(repo_path):
        logger.info(f"Repository path '{repo_path}' already exists. Skipping initialization.")
        return

    try:
        if ssh_key_path:
            if isinstance(ssh_key_path, str):
                ssh_key_path = Path(ssh_key_path)
            if ssh_key_path.exists():
                # Set SSH command for cloning
                logger.info(f"Using SSH key at '{ssh_key_path}' for cloning.")
                os.environ["GIT_SSH_COMMAND"] = f"ssh -i {ssh_key_path}"

        logger.info(f"Performing a shallow clone from '{git_url}' to '{repo_path}' using SSH key.")
        # Shallow clone with specific branch to reduce download & SD writes
        git.Repo.clone_from(git_url, repo_path, branch=git_branch, depth=1)  
        logger.info(f"Repository successfully cloned on branch '{git_branch}'.")
    except Exception as e:
        logger.error(f"{root_cfg.RAISE_WARN()}Failed to clone the repository: {e}")

def refresh_git_repo(git_url: str, git_branch: str = "main", ssh_key_path: Optional[Path|str] = None) -> None:
    """Refresh the Git repository by pulling the latest changes."""
    
    # Get the repo name from the URL
    repo_path = _get_repo_path(git_url)

    if not os.path.exists(repo_path):
        initialize_git_repo(git_url, git_branch, ssh_key_path)

    try:
        repo = git.Repo(repo_path)

        # Ensure the remote URL uses SSH
        remote_url = repo.remotes.origin.url
        if not remote_url.startswith("git@"):
            logger.error(f"{root_cfg.RAISE_WARN()}Remote URL is not configured for SSH access.")
            return

        if ssh_key_path:
            if isinstance(ssh_key_path, str):
                ssh_key_path = Path(ssh_key_path) 
            if ssh_key_path.exists():
                # Set SSH command for pulling
                logger.info(f"Using SSH key at '{ssh_key_path}' for pulling.")
                os.environ["GIT_SSH_COMMAND"] = f"ssh -i {ssh_key_path}"

        logger.info(f"Checking out branch '{git_branch}'.")
        repo.git.checkout(git_branch)  # Ensure the correct branch is checked out

        if repo.is_dirty():
            logger.warning("Repository has uncommitted changes. Stashing changes.")
            repo.git.stash()

        logger.info(f"Pulling latest changes from branch '{git_branch}' in remote repository.")
        repo.remotes.origin.pull(git_branch)  # Pull specific branch
        logger.info(f"Repository successfully updated on branch '{git_branch}'.")
    except Exception as e:
        logger.error(f"{root_cfg.RAISE_WARN()}Failed to refresh code from {git_url}: {e}")

def _get_repo_path(git_url: str) -> Path:
    """Get the path to the Git repository."""
    git_repo_name = os.path.basename(git_url).replace(".git", "")
    repo_path = root_cfg.CODE_DIR / git_repo_name

    return repo_path

def main() -> None:
    """Main function to refresh the Git repository.
    This is called from crontab - which is setup in DeviceManager."""
    if not root_cfg.system_cfg:
        logger.error(f"{root_cfg.RAISE_WARN()}system.cfg does not exist or has not been loaded.")
        return
    
    if ((not root_cfg.system_cfg.my_git_repo_url) or 
        (root_cfg.system_cfg.my_git_repo_url == root_cfg.FAILED_TO_LOAD)):
        logger.error(f"{root_cfg.RAISE_WARN()}my_git_repo_url is not defined in system configuration.")
        return
    
    # Refresh the user's code
    if not root_cfg.system_cfg.my_git_branch or root_cfg.system_cfg.my_git_branch == root_cfg.FAILED_TO_LOAD:
        refresh_git_repo(
            root_cfg.system_cfg.my_git_repo_url,
            root_cfg.system_cfg.my_git_ssh_private_key_file
        )
    else:
        refresh_git_repo(
            root_cfg.system_cfg.my_git_repo_url,
            root_cfg.system_cfg.my_git_branch,
            root_cfg.system_cfg.my_git_ssh_private_key_file
        )

    # Refresh the SensorCore code
    if not root_cfg.system_cfg.sensor_core_git_branch:
        refresh_git_repo(
            root_cfg.system_cfg.sensor_core_git_url,
            root_cfg.system_cfg.my_git_ssh_private_key_file
        )
    else:
        refresh_git_repo(
            root_cfg.system_cfg.sensor_core_git_url,
            root_cfg.system_cfg.sensor_core_git_branch,
            root_cfg.system_cfg.my_git_ssh_private_key_file
        )

if __name__ == "__main__":
    main()
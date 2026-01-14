# RPi Installer Script - Architecture Overview

## Overview

The `rpi_installer.sh` script is a system provisioning and maintenance tool designed to configure Raspberry Pi devices for running the Expidite sensor framework. It serves as both an initial setup script and an automated maintenance system that runs on reboot and at scheduled intervals.

## Key Components & Architecture

### 1. **Configuration Management**
- **System Configuration Reader**: Parses `~/.expidite/system.cfg` to extract deployment parameters
- **Environment Validation**: Validates prerequisites including SSH access, directory structure, and system capabilities
- **Git access**: Handles both SSH (private repositories) and HTTPS (public repositories) Git access

### 2. **Installation Pipeline**
The installer follows a structured pipeline approach:

```
Prerequisites Check → Environment Setup → Package Installation → 
Code Deployment → System Configuration → Service Management → Persistence
```

#### Core Pipeline Functions:
- `check_prerequisites()`: Validates system state and prevents reboot loops
- `export_system_cfg()`: Loads and exports configuration variables
- `create_and_activate_venv()`: Manages Python virtual environment lifecycle
- `install_os_packages()`: Handles system-level package management via apt
- `install_expidite()`: Deploys the core Expidite framework from GitHub
- `install_user_code()`: Deploys custom user sensor/processor code

### 3. **Security & Access Management**
- **SSH Key Management**: Automated deployment of SSH keys for private repository access
- **Firewall Configuration**: UFW (Uncomplicated Firewall) setup with predefined rules
- **Repository Access**: Dynamic URL formatting for both SSH and HTTPS Git operations

### 4. **System Optimization**
#### Storage Optimization:
- **Volatile Logging**: Configures systemd journald to use RAM-based storage
- **RAM Disk Creation**: Creates tmpfs mounts for high-frequency I/O operations
- **SD Card Protection**: Minimizes write operations to extend SD card lifespan

#### Hardware Configuration:
- **I2C Interface**: Enables I2C for sensor communication
- **Network Interface**: Sets predictable network interface names
- **LED Management**: Configures status LED control service

### 5. **Version Control & Update Management**
- **Hash-Based Deployment**: Tracks Git commit hashes to avoid unnecessary reinstalls
- **Atomic Updates**: Only updates when new commits are detected
- **Rollback Safety**: Preserves version information and maintains update history

### 6. **Process Management & Persistence**
- **Cron Integration**: Self-registers for reboot and scheduled maintenance
- **Auto-Start Capability**: Optionally launches sensor applications on boot
- **Service Management**: Manages systemd services for background operations

### 7. **Error Handling & Recovery**
- **Reboot Loop Prevention**: Tracks reboot attempts and disables automatic reboots after failures
- **Network Resilience**: Continues execution despite transient network failures
- **Graceful Degradation**: Non-fatal errors don't prevent subsequent installation steps

## Data Flow Architecture

### Input Sources:
1. **Configuration Files**: `system.cfg`, `keys.env` in `~/.expidite/`
2. **SSH Keys**: Private keys for Git repository access
3. **Git Repositories**: Expidite framework and user code repositories

### Processing Stages:
1. **Environment Preparation**: Virtual environment, SSH keys, system packages
2. **Code Deployment**: Framework and user code installation via pip
3. **System Configuration**: Logging, storage, hardware interfaces
4. **Service Activation**: Background services, auto-start, cron jobs

### Output Artifacts:
1. **Virtual Environment**: Isolated Python environment with installed packages
2. **System Services**: LED manager, firewall, logging configuration
3. **Mount Points**: RAM disk and storage optimizations
4. **Cron Jobs**: Scheduled maintenance and auto-start entries

## Execution Modes

### 1. **Initial Installation**
- Fresh system setup with comprehensive configuration
- Triggers OS updates and package installations
- Creates new virtual environment and installs all components

### 2. **Update Mode** 
- Checks for new commits in repositories
- Selective updates based on hash comparisons
- Preserves existing configuration while updating code

### 3. **Maintenance Mode**
- Scheduled execution via cron (weekly OS updates)
- Automatic execution on system reboot
- Background health checks and recovery operations

## Integration Points

### External Dependencies:
- **GitHub Repositories**: Source for framework and user code
- **System Package Manager**: apt for OS-level dependencies
- **systemd**: Service management and logging configuration
- **cron**: Scheduled execution and persistence

### Configuration Integration:
- **systemd journald**: Log storage configuration
- **UFW Firewall**: Network security rules
- **SSH Configuration**: Key management and known_hosts
- **Virtual Environment**: Python package isolation

## Error Recovery & Resilience

### Network Failure Handling:
- Continues execution despite Git clone/install failures
- Preserves last known good state
- Logs failures without blocking subsequent operations

### Reboot Loop Prevention:
- Tracks reboot attempts with timestamps
- Disables automatic reboots after repeated failures
- Provides manual recovery mechanisms

### State Persistence:
- Maintains installation state in `~/.expidite/flags/`
- Tracks version information and update history
- Preserves configuration across reboots and updates

## Usage Patterns

### Manual Execution:
```bash
# Standard installation/update
./rpi_installer.sh

# Force OS update
./rpi_installer.sh os_update
```

### Automated Execution:
- **On Reboot**: Automatic execution via cron `@reboot`
- **Weekly Maintenance**: Saturday 2 AM OS updates

## Design Principles

1. **Idempotent Operations**: Safe to run multiple times without side effects
2. **Atomic Updates**: Version-controlled deployments with rollback capability
3. **Resource Conservation**: Optimized for SD card longevity and RAM usage
4. **Self-Healing**: Automatic recovery and maintenance capabilities
5. **Security-First**: Proper key management and firewall configuration
6. **Monitoring Integration**: Comprehensive logging and status reporting

This architecture provides a robust, self-maintaining system for deploying and managing Raspberry Pi-based sensor networks in production environments.
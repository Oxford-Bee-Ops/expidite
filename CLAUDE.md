# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

ExPiDITE (Expedited Raspberry Pi Data Intensive Tracking Environment) is a Python framework for autonomous scientific data collection on Raspberry Pi devices. It handles sensor integration, data processing, and Azure cloud storage for IoT/scientific fleet deployments.

Repository: https://github.com/Oxford-Bee-Ops/expidite

## Commands

### Quality Checks (run before committing)

```bash
ruff format               # Code formatting
ruff check --fix          # Linting
ty check                  # Fast type checking
pyright                   # Advanced type checking
mypy                      # Comprehensive type checking
pytest                    # Run unit tests
```

On Windows, `check.cmd` runs the full quality check pipeline.

Optional:
```bash
codespell                 # Spell checking
deadcode .                # Dead code detection
```

### Testing

```bash
pytest                                    # Run all unit tests
pytest test/rpi_core/core/               # Run specific test directory
pytest -m unittest                        # Only quick unit tests
pytest -m systemtest                      # Full system tests (requires test rig)
```

Test markers are defined in `pytest.ini`: `unittest` (quick, always run) and `systemtest` (hardware tests).

## Architecture

### Package Structure

```
src/
  expidite_rpi/     # Main RPi sensor framework (primary package)
    core/           # Core abstractions and orchestration
    sensors/        # Hardware sensor implementations
    utils/          # Utilities, cloud journal, RPi emulator
    scripts/        # Installation and deployment scripts
    example/        # Template code for custom sensors/processors
    bcli.py         # CLI entry point (bcli command)
    rpi_core.py     # Public API (RpiCore class)
```

### Core Data Flow

```
Sensor (Thread) → DataProcessor(s) → Cloud Storage
```

- **Sensor** (`core/sensor.py`): Abstract base inheriting from `Thread` and `DPnode`. Runs continuously, calls `log()` or `save_data()` to emit data downstream.
- **DataProcessor** (`core/dp.py`): Abstract base for transformation steps. Implements `process_data()`.
- **DPtree** (`core/dp_tree.py`): Defines the topology — which sensors feed which processors. Supports linear chains and branching.
- **DPworker** (`core/dp_worker_thread.py`): One worker thread per DPtree; moves data between nodes.
- **DPnode** (`core/dp_node.py`): Base class for both Sensor and DataProcessor. Manages streams, file naming, cloud uploads, and statistics.

### Orchestration

- **EdgeOrchestrator** (`core/edge_orchestrator.py`): Singleton that starts/stops/monitors all sensor threads and DPworker threads. Entry point: `RpiCore` class in `rpi_core.py`.
- **CloudConnector** (`core/cloud_connector.py`): Abstraction over Azure Blob Storage with retry logic, tier management, and sync/async modes.
- **DeviceManager** (`core/device_manager.py`): Fleet device lifecycle (provision, configure, update).

### Configuration System

Configuration flows: YAML/env files → `configuration.py` → `device_config_objects.py` (DeviceCfg, SystemCfg) and `dp_config_objects.py` (SensorCfg, DataProcessorCfg, Stream).

- Device and fleet configuration is version-controlled in a separate Git repo (not this one).
- Environment secrets (Azure keys, GitHub tokens) loaded via pydantic-settings.
- `config_validator.py` validates config before deployment.

### File Naming

`core/file_naming.py` enforces FAIR-principle-compliant file naming across all data outputs. All sensor data files follow a strict naming convention — do not bypass this.

### Testing Without Hardware

`utils/rpi_emulator.py` (22 KB) provides a full hardware emulation framework. Use it in tests to mock sensors and I2C devices. See `docs/rpi_emulator.md` for usage.

### Key Constants

`core/api.py` defines the central constants used across the system: `SENSOR_TYPE`, `FORMAT`, `RECORD_ID`, and system data stream type enums. Reference these rather than using string literals.

## Extending the Framework

- **Custom Sensor**: Subclass `Sensor`, implement `run()`. See `example/my_sensor_example.py`.
- **Custom Processor**: Subclass `DataProcessor`, implement `process_data()`. See `example/my_processor_example.py`.
- **Fleet Config**: See `example/my_fleet_config.py` for the config structure.

## Python Requirements

- Python 3.11+ required
- Strict typing is enforced via mypy + pyright + ty — all code must pass all three type checkers
- `ruff` is both the formatter and linter (replaces black + flake8)

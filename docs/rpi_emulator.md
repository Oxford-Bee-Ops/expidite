# RPI Emulator - Testing Framework Documentation

## Overview

The RPI Emulator (`rpi_emulator.py`) is a testing framework that enables thorough testing of the Expidite sensor framework without requiring actual Raspberry Pi hardware. It provides a controlled testing environment by emulating hardware commands and intercepting cloud operations.

## Architecture

### Key Components

1. **Command Emulation**: Intercepts and emulates Linux sensor commands (rpicam-vid, arecord)
2. **Cloud Connector Mocking**: Uses `LocalCloudConnector` instead of Azure blob storage
3. **Recording Management**: Manages test recordings and validates expected outputs
4. **Timer Mocking**: Accelerates system timers for faster test execution

### Design Pattern

The RPI Emulator can be used in two ways:

**Pytest Fixture (Recommended):**
```python
@pytest.mark.unittest
def test_my_sensor(self, rpi_emulator):
    # Test code here - emulator is provided as parameter
    # Resources are automatically managed by pytest
```

**Context Manager (Alternative):**
```python
with RpiEmulator.get_instance() as rpi_emulator:
    # Test code here - emulator is active
    # Resources are automatically cleaned up on exit
```

## Core Features

### 1. Hardware Command Emulation

The emulator intercepts calls to `utils.run_cmd()` and provides realistic responses for sensor hardware commands.

#### Supported Commands

- **rpicam-vid**: Video recording emulation
  - Creates dummy video files or uses provided test recordings
  - Supports parameter extraction (duration, framerate, resolution)
  - Simulates recording time delays

- **arecord**: Audio recording emulation
  - Creates audio files from test recordings or generates placeholders
  - Handles duration and format parameters
  - Simulates recording delays

### 2. Test Recording Management

#### Setting Test Recordings

```python
from expidite_rpi.utils.rpi_emulator import RpiTestRecording

# Define test recordings
test_recordings = [
    RpiTestRecording(
        cmd_prefix="rpicam-vid",
        recordings=[
            Path("test/resources/sample_video.mp4"),
            Path("test/resources/another_video.mp4")
        ]
    )
]

# Using pytest fixture
@pytest.mark.unittest
def test_with_recordings(self, rpi_emulator):
    rpi_emulator.set_recordings(test_recordings)
    # When sensors run rpicam-vid commands, they'll get these recordings
```

#### Recording Caps and Limits

```python
# Using pytest fixture
@pytest.mark.unittest
def test_with_caps(self, rpi_emulator):
    # Limit total recordings across all sensors
    rpi_emulator.set_recording_cap(5)

    # Limit recordings for specific data types
    rpi_emulator.set_recording_cap(3, type_id="RPICAM")
    rpi_emulator.set_recording_cap(2, type_id="AUDIO")
```### 3. Cloud Storage Emulation

The emulator automatically configures a local filesystem-based cloud connector:

- Files are stored locally instead of Azure blob storage
- Container structure is preserved
- Upload/download operations work normally
- Data validation remains identical to production

### 4. System Timer Acceleration

For faster test execution, system timers are mocked:

```python
# Production timers (seconds)
DP_FREQUENCY = 60
JOURNAL_SYNC_FREQUENCY = 180

# Test timers (accelerated)
DP_FREQUENCY = 1
JOURNAL_SYNC_FREQUENCY = 1
```

## Usage Patterns

### Method 1: Pytest Fixture (Recommended)

The cleanest approach uses a pytest fixture to inject the RpiEmulator instance:

```python
import pytest
from expidite_rpi.rpi_core import RpiCore
from expidite_rpi.core.device_config_objects import DeviceCfg

class TestMySensor:
    @pytest.fixture
    def inventory(self):
        return [
            DeviceCfg(
                name="TestDevice",
                device_id="d01111111111",
                dp_trees_create_method=create_test_device,
            ),
        ]

    @pytest.mark.unittest
    def test_sensor_operation(self, rpi_emulator):
        # Configure test environment
        rpi_emulator.set_recording_cap(1, type_id="MYSENSOR")

        # Set up RpiCore with mocked inventory (timers already mocked)
        sc = RpiCore()
        sc.configure(rpi_emulator.inventory)
        sc.start()

        # Wait for expected operations
        while not rpi_emulator.recordings_cap_hit(type_id="MYSENSOR"):
            time.sleep(0.1)

        # Clean shutdown
        sc.stop()

        # Validate results
        rpi_emulator.assert_records("expidite-upload", {"V3_MYSENSOR*": 1})
```

**Note**: The `rpi_emulator` fixture automatically:
- Handles the context manager lifecycle
- Requires an `inventory` fixture to be defined in your test
- Automatically applies `mock_timers` to your inventory
- Stores the mocked inventory in `rpi_emulator.inventory`

### Method 2: Context Manager (Alternative)

For cases where you need more control over the emulator lifecycle:

```python
class TestMySensor:
    @pytest.mark.unittest
    def test_sensor_operation(self):
        with RpiEmulator.get_instance() as rpi_emulator:
            # Configure test environment
            rpi_emulator.set_recording_cap(1, type_id="MYSENSOR")

            # Rest of test code...
```### Device Testing Pattern

```python
from expidite_rpi.core.device_config_objects import DeviceCfg

class TestCameraDevice:
    @pytest.fixture
    def inventory(self):
        return [
            DeviceCfg(
                name="TestDevice",
                device_id="d01111111111",  # Standard test device ID
                notes="Testing camera device",
                dp_trees_create_method=create_camera_device,
            ),
        ]

    @pytest.mark.unittest
    def test_camera_device(self, rpi_emulator):
        # Inventory is already mocked with timers via rpi_emulator fixture

        # Configure RpiCore
        sc = RpiCore()
        sc.configure(rpi_emulator.inventory)
        sc.start()

        # Wait for processing to complete
        time.sleep(2)
        sc.stop()

        # Validate expected outputs
        rpi_emulator.assert_records("expidite-upload", {"V3_RPICAM*": 1})
```

### Using Pre-recorded Test Data

```python
from expidite_rpi.utils.rpi_emulator import RpiTestRecording

class TestVideoProcessor:
    @pytest.mark.unittest
    def test_video_processing(self, rpi_emulator):
        # Use actual video file for testing
        rpi_emulator.set_recordings([
            RpiTestRecording(
                cmd_prefix="rpicam-vid",
                recordings=[
                    root_cfg.TEST_DIR / "resources" / "test_video.mp4"
                ]
            )
        ])

        # Rest of test...
```

## Validation Methods

### File and Data Validation

```python
# Validate expected number of files
rpi_emulator.assert_records(
    container="expidite-upload",
    expected_files={"V3_CAMERA*": 2, "V3_AUDIO*": 1}
)

# Validate file contents (for CSV/journal files)
rpi_emulator.assert_records(
    container="expidite-journals",
    expected_files={"V3_SENSOR*": 1},
    expected_rows={"V3_SENSOR*": 10}  # 10 data rows (excluding header)
)

# Get data for custom validation
df = rpi_emulator.get_journal_as_df("expidite-journals", "V3_TEMPERATURE")
assert df["temperature"].mean() > 20.0
```

### Processing Status Checks

```python
# Check if recording cap reached for specific sensor type
while not rpi_emulator.recordings_cap_hit(type_id="RPICAM"):
    time.sleep(0.1)

# Check if all files have been processed
while rpi_emulator.recordings_still_to_process():
    time.sleep(0.1)
```

## Configuration Guidelines

### Test Device ID

Always use the standard test device ID: `"d01111111111"`

```python
root_cfg.update_my_device_id("d01111111111")
```

### Pytest Markers

The framework supports different test categories:

```python
@pytest.mark.unittest      # Quick tests, always run
@pytest.mark.systemtest    # Full integration tests
```

Configure in `pytest.ini`:
```ini
[pytest]
markers =
    unittest: quick tests to always run
    systemtest: full tests to run on system test rig
```

### Test Resource Management

```python
# Ensure clean test environment
@pytest.fixture(autouse=True)
def shutdown_cloud_connector():
    yield
    try:
        cc = CloudConnector.get_instance(root_cfg.CloudType.AZURE)
        cc.shutdown()
        time.sleep(1)
    except Exception:
        pass
```

## Best Practices

### 1. Use Pytest Fixtures (Recommended)

Use the `rpi_emulator` fixture for cleaner test code:

```python
# ✅ Recommended - Clean and minimal indentation
@pytest.mark.unittest
def test_my_sensor(self, rpi_emulator):
    rpi_emulator.set_recording_cap(1, type_id="MYSENSOR")
    # Test code here

# ✅ Alternative - Context manager for advanced usage
def test_advanced_scenario(self):
    with RpiEmulator.get_instance() as rpi_emulator:
        # Test code when you need custom lifecycle control

# ❌ Avoid - Manual instance management
rpi_emulator = RpiEmulator.get_instance()  # Resources not cleaned up
```

### 2. Define Inventory Fixture

Always define an `inventory` fixture in your test:

```python
@pytest.fixture
def inventory(self):
    return [
        DeviceCfg(
            name="TestDevice",
            device_id="d01111111111",
            dp_trees_create_method=create_test_device,
        ),
    ]
```

### 3. Use Mocked Inventory

The `rpi_emulator` fixture automatically mocks timers:

```python
# Timers are already mocked - use the mocked inventory
sc.configure(rpi_emulator.inventory)
```

### 4. Proper Cleanup

Ensure sensors are properly stopped:

```python
sc.start()
try:
    # Test operations
    pass
finally:
    sc.stop()  # Always stop the sensor core
```

### 4. Platform-Specific Tests

Handle platform differences gracefully:

```python
if root_cfg.running_on_windows:
    logger.warning("Skipping I2C test on Windows")
    return
```

## Debugging Tests

### Enable Debug Logging

```python
import logging
logger = root_cfg.setup_logger("expidite", level=logging.DEBUG)
```

### Inspect Local Cloud Storage

After test execution, examine the local cloud directory:

```python
# Files are stored in the local cloud directory
local_cloud = rpi_emulator.cc.get_local_cloud()
print(f"Local cloud files: {list(local_cloud.rglob('*'))}")
```

### Test Recording Information

```python
# Check what recordings were used
print(f"Recordings saved: {rpi_emulator.recordings_saved}")
print(f"Current recording index: {rpi_emulator.previous_recordings_index}")
```

## Common Test Scenarios

### Testing Continuous Sensors

```python
class TestContinuousSensor:
    @pytest.fixture
    def inventory(self):
        return [
            DeviceCfg(
                name="ContinuousSensor",
                device_id="d01111111111",
                dp_trees_create_method=create_continuous_sensor,
            ),
        ]

    @pytest.mark.unittest
    def test_continuous_operation(self, rpi_emulator):
        rpi_emulator.set_recording_cap(3, type_id="SENSOR_DATA")

        sc = RpiCore()
        sc.configure(rpi_emulator.inventory)
        sc.start()

        # Wait for multiple recordings
        while not rpi_emulator.recordings_cap_hit(type_id="SENSOR_DATA"):
            time.sleep(0.1)

        sc.stop()
        rpi_emulator.assert_records("expidite-journals", {"V3_SENSOR*": 3})
```

### Testing On-Demand Sensors

```python
class TestOnDemandSensor:
    @pytest.fixture
    def inventory(self):
        return [
            DeviceCfg(
                name="OnDemandSensor",
                device_id="d01111111111",
                dp_trees_create_method=create_on_demand_sensor,
            ),
        ]

    @pytest.mark.unittest
    def test_triggered_sensing(self, rpi_emulator):
        sc = RpiCore()
        sc.configure(rpi_emulator.inventory)
        sc.start()

        # Trigger sensing via flag file
        with open(root_cfg.SENSOR_TRIGGER_FLAG, "w") as f:
            f.write("30")  # 30 second recording

        # Wait for completion
        time.sleep(2)
        sc.stop()

        rpi_emulator.assert_records("expidite-upload", {"V3_VIDEOOD*": 1})
```

### Testing Data Processors

```python
class TestDataProcessor:
    @pytest.fixture
    def inventory(self):
        return [
            DeviceCfg(
                name="VideoProcessor",
                device_id="d01111111111",
                dp_trees_create_method=create_video_processor,
            ),
        ]

    @pytest.mark.unittest
    def test_video_processing(self, rpi_emulator):
        # Provide specific test video
        rpi_emulator.set_recordings([
            RpiTestRecording(
                cmd_prefix="rpicam-vid",
                recordings=[test_video_path]
            )
        ])

        rpi_emulator.set_recording_cap(1)
        sc = RpiCore()
        sc.configure(rpi_emulator.inventory)
        sc.start()

        # Wait for processing chain completion
        while not rpi_emulator.recordings_cap_hit(type_id="RPICAM"):
            time.sleep(0.1)
        while rpi_emulator.recordings_still_to_process():
            time.sleep(0.1)

        sc.stop()

        # Validate processor outputs
        rpi_emulator.assert_records("expidite-upload", {"V3_PROCESSED*": 1})
```

## Error Handling

### Common Issues and Solutions

1. **Recording Cap Not Set**
   ```python
   # Error: "The recording cap is not set for that type_id"
   # Solution: Always set recording caps before starting tests
   rpi_emulator.set_recording_cap(1, type_id="SENSOR_TYPE")
   ```

2. **Missing Test Recordings**
   ```python
   # Error: "Recording not found for command"
   # Solution: Provide test recordings or allow dummy generation
   rpi_emulator.set_recordings([...])  # or let emulator generate dummy files
   ```

3. **Test Hanging**
   ```python
   # Issue: Tests wait forever for recording caps
   # Solution: Check that sensors are actually running and producing data
   if root_cfg.running_on_windows and sensor_requires_hardware:
       pytest.skip("Requires hardware not available on Windows")
   ```

## Integration with CI/CD

The RPI Emulator is designed to run in CI/CD environments:

- No external dependencies on hardware
- Deterministic test execution
- Fast execution with mocked timers
- Clean resource management
- Cross-platform compatibility

```yaml
# Example GitHub Actions
- name: Run Unit Tests
  run: python -m pytest -m unittest

- name: Run System Tests
  run: python -m pytest -m systemtest
```

This testing framework enables comprehensive validation of the entire Expidite sensor ecosystem while maintaining fast, reliable, and platform-independent test execution.
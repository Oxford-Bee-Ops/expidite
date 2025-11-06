"""
Simple test script to exercise the review mode functionality of RpicamSensor.

This test demonstrates the review mode behavior by:
1. Creating a sensor with proper stream configuration
2. Mocking the review mode flag file to simulate review mode activation
3. Testing the review_mode_output() method directly
4. Testing the main run() loop with review mode enabled
"""

import tempfile
import time
from pathlib import Path
from unittest.mock import patch

from expidite_rpi.core import api
from expidite_rpi.core import configuration as root_cfg
from expidite_rpi.sensors.sensor_rpicam_vid import (
    DEFAULT_RPICAM_SENSOR_CFG,
    RPICAM_REVIEW_MODE_STREAM_INDEX,
    RpicamSensor,
    RpicamSensorCfg,
)


class TestReviewModeExercise:
    """Test class for exercising review mode functionality."""

    def setup_method(self):
        """Set up test environment before each test."""
        # Enable testing mode so sensor doesn't check for Raspberry Pi
        root_cfg.ST_MODE = root_cfg.SOFTWARE_TEST_MODE.TESTING
        
        # Create a temporary directory for test files
        self.temp_dir = Path(tempfile.mkdtemp())
        
        # Mock the review mode flag file path
        self.review_flag_path = self.temp_dir / "review_mode_flag"

    def teardown_method(self):
        """Clean up after each test."""
        # Clean up temp files
        if self.review_flag_path.exists():
            self.review_flag_path.unlink()
        
        # Reset testing mode
        root_cfg.ST_MODE = root_cfg.SOFTWARE_TEST_MODE.LIVE

    @patch('expidite_rpi.utils.utils.run_cmd')
    @patch('expidite_rpi.core.file_naming.get_temporary_filename')
    @patch.object(RpicamSensor, 'save_recording')
    def test_review_mode_output_direct_call(self, mock_save_recording, mock_get_filename, mock_run_cmd):
        """Test calling review_mode_output() directly."""
        # Arrange
        test_file = self.temp_dir / "test_review_image.jpg"
        mock_get_filename.return_value = test_file
        mock_run_cmd.return_value = 0  # Successful command execution
        
        sensor = RpicamSensor(DEFAULT_RPICAM_SENSOR_CFG)
        
        # Act
        sensor.review_mode_output()
        
        # Assert
        mock_get_filename.assert_called_once_with(api.FORMAT.JPG)
        mock_run_cmd.assert_called_once()
        mock_save_recording.assert_called_once_with(
            RPICAM_REVIEW_MODE_STREAM_INDEX,
            test_file,
            start_time=mock_save_recording.call_args[1]['start_time']
        )
        
        # Verify the command contains the review mode command
        called_command = mock_run_cmd.call_args[0][0]
        assert "rpicam-still" in called_command
        assert str(test_file) in called_command

    @patch('expidite_rpi.core.configuration.REVIEW_MODE_FLAG')
    @patch('expidite_rpi.utils.utils.run_cmd')
    @patch('expidite_rpi.core.file_naming.get_temporary_filename')
    @patch.object(RpicamSensor, 'save_recording')
    def test_sensor_run_in_review_mode(self, mock_save_recording, mock_get_filename, 
                                       mock_run_cmd, mock_review_flag):
        """Test the main sensor run() loop with review mode activated."""
        # Arrange
        mock_review_flag.exists.return_value = True
        mock_review_flag.stat.return_value.st_mtime = time.time()  # Current timestamp
        
        mock_get_filename.return_value = Path("/tmp/test_review_image.jpg")
        mock_run_cmd.return_value = 0
        
        sensor = RpicamSensor(DEFAULT_RPICAM_SENSOR_CFG)
        
        # Mock continue_recording to run only a few iterations
        call_count = 0
        def mock_continue_recording():
            nonlocal call_count
            call_count += 1
            return call_count <= 3  # Run 3 iterations then stop
        
        with patch.object(sensor, 'continue_recording', side_effect=mock_continue_recording):
            with patch.object(sensor.stop_requested, 'wait') as mock_wait:
                # Act
                sensor.run()
        
        # Assert
        # Should have called review_mode_output 3 times (once per iteration)
        assert mock_save_recording.call_count == 3
        
        # All calls should be for review mode stream
        for call in mock_save_recording.call_args_list:
            assert call[0][0] == RPICAM_REVIEW_MODE_STREAM_INDEX
        
        # Should have waited between iterations
        assert mock_wait.call_count == 3

    @patch.object(RpicamSensor, 'in_review_mode')
    @patch('expidite_rpi.utils.utils.run_cmd')
    @patch('expidite_rpi.core.file_naming.get_temporary_filename')
    @patch.object(RpicamSensor, 'save_recording')
    def test_sensor_toggles_between_normal_and_review_mode(self, mock_save_recording, 
                                                          mock_get_filename, mock_run_cmd, 
                                                          mock_in_review_mode):
        """Test sensor behavior when toggling between normal and review modes."""
        # Arrange
        test_file = self.temp_dir / "test_file.mp4"
        mock_get_filename.return_value = test_file
        mock_run_cmd.return_value = 0
        
        sensor = RpicamSensor(DEFAULT_RPICAM_SENSOR_CFG)
        
        # Simulate toggling review mode on/off
        review_mode_states = [False, True, True, False]  # Normal, Review, Review, Normal
        state_index = 0
        
        def mock_review_mode():
            nonlocal state_index
            if state_index < len(review_mode_states):
                result = review_mode_states[state_index]
                state_index += 1
                return result
            return False
        
        mock_in_review_mode.side_effect = mock_review_mode
        
        call_count = 0
        def mock_continue_recording():
            nonlocal call_count
            call_count += 1
            return call_count <= 4  # Run 4 iterations
        
        with patch.object(sensor, 'continue_recording', side_effect=mock_continue_recording):
            with patch.object(sensor.stop_requested, 'wait'):
                # Act
                sensor.run()
        
        # Assert
        assert mock_save_recording.call_count == 4
        
        # Check stream indices used - should alternate between normal and review modes
        stream_indices_used = [call[0][0] for call in mock_save_recording.call_args_list]
        expected_indices = [0, 1, 1, 0]  # Normal, Review, Review, Normal
        assert stream_indices_used == expected_indices

    @patch('expidite_rpi.utils.utils.run_cmd')
    def test_review_mode_command_construction(self, mock_run_cmd):
        """Test that review mode constructs the correct rpicam-still command."""
        # Arrange
        custom_review_cmd = "rpicam-still --width 1280 --height 720 --quality 95 -o FILENAME"
        
        config = RpicamSensorCfg(
            sensor_type=api.SENSOR_TYPE.CAMERA,
            sensor_index=0,
            sensor_model="HighResCamera",
            description="High resolution camera",
            outputs=DEFAULT_RPICAM_SENSOR_CFG.outputs,
            review_mode_cmd=custom_review_cmd
        )
        
        sensor = RpicamSensor(config)
        
        with patch('expidite_rpi.core.file_naming.get_temporary_filename') as mock_get_filename:
            with patch.object(sensor, 'save_recording'):
                test_file = self.temp_dir / "custom_review.jpg"
                mock_get_filename.return_value = test_file
                mock_run_cmd.return_value = 0
                
                # Act
                sensor.review_mode_output()
        
        # Assert
        called_command = mock_run_cmd.call_args[0][0]
        expected_command = f"rpicam-still --width 1280 --height 720 --quality 95 -o {test_file}"
        assert called_command == expected_command


if __name__ == "__main__":
    # Simple test runner for direct execution
    import sys
    
    print("Running Review Mode Exercise Tests...")
    
    test_instance = TestReviewModeExercise()
    
    try:
        # Test 1: Direct review mode output call
        print("\n1. Testing direct review_mode_output() call...")
        test_instance.setup_method()
        test_instance.test_review_mode_output_direct_call()
        test_instance.teardown_method()
        print("   ✓ Passed")
        
        # Test 2: Main run loop in review mode
        print("\n2. Testing sensor run() loop with review mode...")
        test_instance.setup_method()
        test_instance.test_sensor_run_in_review_mode()
        test_instance.teardown_method()
        print("   ✓ Passed")
        
        # Test 3: Toggle between modes
        print("\n3. Testing toggle between normal and review modes...")
        test_instance.setup_method()
        test_instance.test_sensor_toggles_between_normal_and_review_mode()
        test_instance.teardown_method()
        print("   ✓ Passed")
        
        # Test 4: Custom command construction
        print("\n4. Testing custom review command construction...")
        test_instance.setup_method()
        test_instance.test_review_mode_command_construction()
        test_instance.teardown_method()
        print("   ✓ Passed")
        
        print("\n🎉 All tests passed!")
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
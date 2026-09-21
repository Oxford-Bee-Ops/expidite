##############################################################################################################
# ContinuousAudioSensor
#
# Called by RpiCore to record audio continuously from USB microphones plugged into the Raspberry Pi.
# Recording is split into consecutive chunks of DeviceCfg.max_recording_timer seconds so that the sensor
# can be cleanly stopped/restarted within the expected shutdown window.
#
# Only supports 1 microphone.
##############################################################################################################
from dataclasses import dataclass

from expidite_rpi.core import api, file_naming
from expidite_rpi.core import configuration as root_cfg
from expidite_rpi.core.dp_config_objects import Stream
from expidite_rpi.core.sensor import Sensor, SensorCfg
from expidite_rpi.utils import utils

logger = root_cfg.setup_logger("expidite")

AUDIO_TYPE_ID = "CONTINUOUSAUDIO"
AUDIO_SENSOR_STREAM_INDEX = 0
AUDIO_SENSOR_STREAM = Stream(
    description="Continuous audio recording.",
    type_id=AUDIO_TYPE_ID,
    index=AUDIO_SENSOR_STREAM_INDEX,
    format=api.FORMAT.WAV,
    cloud_container="expidite-upload",
    sample_probability="1.0",
)


@dataclass
class ContinuousAudioSensorCfg(SensorCfg):
    ##########################################################################################################
    # Custom fields
    ##########################################################################################################
    # arecord command to call with the following placeholders:
    # HW_INDEX: dynamically replaced with the hardware index of the USB microphone to use.
    # DURATION: The duration of each audio recording chunk in seconds (set to max_recording_timer).
    # FILENAME: The filename to use for the audio recording.
    arecord_cmd: str = "arecord -D HW_INDEX -r 44100 -c 1 -f S16_LE -t wav -d DURATION FILENAME"


DEFAULT_CONTINUOUS_AUDIO_SENSOR_CFG = ContinuousAudioSensorCfg(
    sensor_type=api.SENSOR_TYPE.USB,
    sensor_index=1,
    sensor_model="USBAudioSensor",
    description="Continuous audio sensor",
    outputs=[AUDIO_SENSOR_STREAM],
)


##############################################################################################################
# The ContinuousAudioSensor class is used to manage continuous, chunked audio recording
##############################################################################################################
class ContinuousAudioSensor(Sensor):
    # Constructor for the ContinuousAudioSensor class
    def __init__(self, config: ContinuousAudioSensorCfg) -> None:
        super().__init__(config)
        self.config: ContinuousAudioSensorCfg = config
        # Check that the arecord_cmd is valid
        assert self.config.arecord_cmd, (
            f"arecord_cmd must be set in the sensor configuration: {self.config.arecord_cmd}"
        )
        assert self.config.arecord_cmd.startswith("arecord "), (
            f"arecord_cmd must start with 'arecord ': {self.config.arecord_cmd}"
        )
        assert "HW_INDEX" in self.config.arecord_cmd, "arecord_cmd must contain the HW_INDEX placeholder"
        assert "FILENAME" in self.config.arecord_cmd, "arecord_cmd must contain the FILENAME placeholder"
        assert "DURATION" in self.config.arecord_cmd, "arecord_cmd must contain the DURATION placeholder"

        # Validate that the required streams exist in the configuration
        try:
            self.get_stream(AUDIO_SENSOR_STREAM_INDEX)  # Main audio stream
        except ValueError as e:
            msg = (
                f"ContinuousAudioSensor requires a main audio stream at index "
                f"{AUDIO_SENSOR_STREAM_INDEX}: {e}"
            )
            raise ValueError(msg) from e

    ##########################################################################################################
    # Main loop that records audio continuously in chunks of max_recording_timer seconds.
    ##########################################################################################################
    def run(self) -> None:
        """Continuously record audio in chunks of max_recording_timer seconds."""
        if not root_cfg.running_on_rpi and root_cfg.ST_MODE != root_cfg.SOFTWARE_TEST_MODE.TESTING:
            logger.warning("Audio recording is only supported on Raspberry Pi.")
            return

        exception_count = 0

        while self.continue_recording():
            try:
                duration = root_cfg.my_device.max_recording_timer

                # Find a USB audio device
                usb_str = utils.run_cmd("arecord -l | grep -i usb")
                if not usb_str:
                    msg = "No USB audio device found for recording"
                    raise RuntimeError(msg)
                card_index = usb_str.split("card ")[1].split(":")[0].strip()
                device_index = usb_str.split("device ")[1].split(":")[0].strip()
                hw_index = f"hw:{card_index},{device_index}"

                start_time = api.utc_now()
                wav_output_filename = file_naming.get_temporary_filename(api.FORMAT.WAV)
                arecord_cmd = self.config.arecord_cmd
                arecord_cmd = arecord_cmd.replace("HW_INDEX", hw_index)
                arecord_cmd = arecord_cmd.replace("DURATION", str(duration))
                arecord_cmd = arecord_cmd.replace("FILENAME", str(wav_output_filename))

                logger.info(f"Recording audio chunk with command: {arecord_cmd}")
                outcome = utils.run_cmd(arecord_cmd, timeout=duration + 60)
                logger.info(f"Audio recording chunk completed: {outcome}")

                final_output_filename = self.save_recording(
                    stream_index=AUDIO_SENSOR_STREAM_INDEX,
                    temporary_file=wav_output_filename,
                    start_time=start_time,
                    end_time=api.utc_now(),
                    can_discard=True,
                )

                logger.info(f"Saved audio chunk of {duration!s}s to {final_output_filename}")
                exception_count = 0  # Reset exception count on success

            except Exception:
                logger.exception(f"{root_cfg.RAISE_WARN()}Error in ContinuousAudioSensor")
                exception_count += 1

                # On the assumption that the error is transient, we will continue to run but sleep for 60s
                self.stop_requested.wait(60)
                if exception_count > 30:
                    logger.exception(f"ContinuousAudioSensor has failed {exception_count} times. Exiting.")
                    self.sensor_failed()
                    break

        logger.warning("Exiting ContinuousAudioSensor loop")

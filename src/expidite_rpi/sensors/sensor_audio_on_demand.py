############################################################################################################
# AudioSensor
#
# Called by RpiCore to record audio from USB microphones plugged into the Raspberry Pi.
#
# Only supports 1 microphone.
############################################################################################################
from dataclasses import dataclass

from expidite_rpi.core import api, file_naming
from expidite_rpi.core import configuration as root_cfg
from expidite_rpi.core.dp_config_objects import Stream
from expidite_rpi.core.sensor import Sensor, SensorCfg
from expidite_rpi.utils import utils

logger = root_cfg.setup_logger("expidite")

AUDIO_TYPE_ID = "AUDIO"
AUDIO_SENSOR_STREAM_INDEX = 0
AUDIO_SENSOR_STREAM = Stream(
    description="On-demand audio recording.",
    type_id=AUDIO_TYPE_ID,
    index=AUDIO_SENSOR_STREAM_INDEX,
    format=api.FORMAT.WAV,
    cloud_container="expidite-od",
    sample_probability="1.0",
)


@dataclass
class AudioSensorCfg(SensorCfg):
    ############################################################
    # Custom fields
    ############################################################
    # arecord command to call with the following placeholders:
    # HW_INDEX: dynamically replaced with the hardware index of the USB microphone to use.
    # DURATION: The duration of each audio recording in seconds.
    # FILENAME: The filename to use for the audio recording.
    arecord_cmd: str = "arecord -D HW_INDEX -r 44100 -c 1 -f S16_LE -t wav -d DURATION FILENAME"


DEFAULT_AUDIO_SENSOR_CFG = AudioSensorCfg(
    sensor_type=api.SENSOR_TYPE.USB,
    sensor_index=1,
    sensor_model="USBAudioSensor",
    description="On-demand audio sensor",
    outputs=[AUDIO_SENSOR_STREAM],
)


############################################################################################################
# The AudioSensor class is used to manage the audio recording
############################################################################################################
class AudioSensor(Sensor):
    # Constructor for the AudioSensor class
    def __init__(self, config: AudioSensorCfg) -> None:
        super().__init__(config)
        self.config: AudioSensorCfg = config
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
            raise ValueError(
                f"AudioSensor requires a main audio stream at index {AUDIO_SENSOR_STREAM_INDEX}: {e}"
            )

    ############################################################################################################
    # Function that records audio on demand.
    ############################################################################################################
    def sensing_triggered(self, duration: int) -> None:
        """Record audio for the specified duration in seconds."""
        if not root_cfg.running_on_rpi and root_cfg.ST_MODE != root_cfg.SOFTWARE_TEST_MODE.TESTING:
            logger.warning("Audio recording is only supported on Raspberry Pi.")
            return

        logger.info(f"AudioSensor triggered to record for {duration!s} seconds")

        try:
            # Find a USB audio device
            usb_str = utils.run_cmd("arecord -l | grep -i usb")
            if not usb_str:
                raise RuntimeError("No USB audio device found for recording")
            card_index = usb_str.split("card ")[1].split(":")[0].strip()
            device_index = usb_str.split("device ")[1].split(":")[0].strip()
            hw_index = f"hw:{card_index},{device_index}"

            start_time = api.utc_now()
            wav_output_filename = file_naming.get_temporary_filename(api.FORMAT.WAV)
            arecord_cmd = self.config.arecord_cmd
            arecord_cmd = arecord_cmd.replace("HW_INDEX", hw_index)
            arecord_cmd = arecord_cmd.replace("DURATION", str(duration))
            arecord_cmd = arecord_cmd.replace("FILENAME", str(wav_output_filename))

            logger.info(f"Recording audio with command: {arecord_cmd}")
            outcome = utils.run_cmd(arecord_cmd)
            logger.info(f"Audio recording completed: {outcome}")

            final_output_filename = self.save_recording(
                stream_index=AUDIO_SENSOR_STREAM_INDEX,
                temporary_file=wav_output_filename,
                start_time=start_time,
                end_time=api.utc_now(),
            )

            logger.info(f"Saved audio of {duration!s}s to {final_output_filename}; ")
        except Exception as e:
            logger.error(f"{root_cfg.RAISE_WARN()}Error in AudioSensor: {e}", exc_info=True)

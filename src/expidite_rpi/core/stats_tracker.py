from datetime import datetime

from expidite_rpi.core import api
from expidite_rpi.core import configuration as root_cfg
from expidite_rpi.core.dp_config_objects import Stream
from expidite_rpi.core.dp_worker_thread import DPworker
from expidite_rpi.core.sensor import Sensor, SensorCfg

logger = root_cfg.setup_logger("expidite")


##############################################################################################################
# Datastreams produced by the RpiCore system
##############################################################################################################

# SCORE - DatastreamType for recording sample counts from the data pipeline
SCORE_FIELDS = [
    "observed_type_id",
    "observed_sensor_index",
    "sample_period",
    "count",
]
# SCORP - special DatastreamType for recording performance of the data pipeline
SCORP_FIELDS = ["data_processor_id", "observed_type_id", "observed_sensor_index", "count", "duration"]

SC_TRACKING_CFG = SensorCfg(
    sensor_type=api.SENSOR_TYPE.SYS,
    sensor_index=0,
    sensor_model="SelfTracker",
    description="RpiCore self-telemetry",
    outputs=[
        Stream(
            "System datastream of DataProcessor performance data",
            api.SCORP_DS_TYPE_ID,
            api.SCORP_STREAM_INDEX,
            format=api.FORMAT.LOG,
            fields=SCORP_FIELDS,
            cloud_container=root_cfg.my_device.cc_for_system_records,
        ),
        Stream(
            "System datastream of count data of records saved to streams",
            api.SCORE_DS_TYPE_ID,
            api.SCORE_STREAM_INDEX,
            format=api.FORMAT.LOG,
            fields=SCORE_FIELDS,
            cloud_container=root_cfg.my_device.cc_for_system_records,
        ),
    ],
)


class StatTracker(Sensor):
    """A special Sensor class that is used to track the performance of the RpiCore system.

    It is not a physical sensor, but is used to track the performance of the system.
    """

    def __init__(self) -> None:
        super().__init__(SC_TRACKING_CFG)
        self.last_ran: datetime = api.utc_now()

    def set_dpworkers(self, dpworkers: list[DPworker]) -> None:
        """Set the DPworker for the SelfTracking sensor.

        This method is called by the EdgeOrchestrator when the SelfTracking is started.
        """
        self.dpworkers = dpworkers

    def run(self) -> None:
        """Main loop for the StatTracker.
        This method is called when the thread is started.
        It runs in a loop, logging health data and warnings at regular intervals.
        """
        try:
            logger.info(f"Starting SelfTracker thread {self!r}")

            while not self.stop_requested.is_set():
                logger.debug(
                    f"SelfTracker {self.sensor_index} running log_sample_data() "
                    f"for {len(self.dpworkers)} DP engines"
                )
                # Trigger each datastream to log sample counts
                for dpworker in self.dpworkers:
                    dpworker.log_sample_data(self.last_ran)

                # Set timer for next run
                self.last_ran = api.utc_now()
                self.stop_requested.wait(root_cfg.my_device.heart_beat_frequency)
        except Exception:
            logger.exception(f"{root_cfg.RAISE_WARN()}Error in SelfTracker thread")

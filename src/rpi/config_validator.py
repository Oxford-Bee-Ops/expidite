####################################################################################################
# The config_validator is used to validate SensorCore configuration files (eg fleet_config.py).
####################################################################################################
from abc import ABC, abstractmethod

from sensor_core import api
from sensor_core import configuration as root_cfg
from sensor_core.cloud_connector import CloudConnector
from sensor_core.dp_config_objects import Stream
from sensor_core.dp_node import DPnode
from sensor_core.dp_tree import DPtree
from sensor_core.sensor import SensorCfg

logger = root_cfg.setup_logger("sensor_core")

class ValidationRule(ABC):
    """ Base class for validation rules. Extend this class to implement specific rules. """
    @abstractmethod
    def validate(self, dpnode: DPnode) -> tuple[bool, str]:
        """
        Validate the configuration.

        Args:
            dpnode (DPtreeNode): The configuration to validate.

        Returns:
            tuple: (bool, str) where the boolean indicates success (True) or failure (False),
                   and the string contains an error message if validation fails.
        """
        raise NotImplementedError("Subclasses must implement the validate method.")

###########################################################################################################
# Start with the device-level validation rules.
###########################################################################################################

# Rule 1: check that the outputs list is not empty
class Rule1_outputs_not_empty(ValidationRule):
    def validate(self, dpnode: DPnode) -> tuple[bool, str]:
        outputs: list = dpnode.get_config().outputs
        if outputs is None or len(outputs) == 0:
            return False, (
                f"Outputs list is empty in {dpnode}: "
                "all DPtree nodes must have at least one output.")
        
        for stream in outputs:
            if not isinstance(stream, Stream):
                return False, (
                    f"Outputs list contains non-Stream object in {dpnode}: "
                    "all DPtree nodes must have at least one output of type Stream.")
            # The stream_index must match the location in the outputs list
            if stream.index != outputs.index(stream):
                return False, (
                    f"The Stream with index {stream.index} is not at that position in the outputs array. "
                    f"Make sure that Streams are declared in the right order, starting with index 0. "
                    f"{dpnode}")

        return True, ""

# Rule 2: check that the sensor model is set for all datastreams
class Rule2_sensor_type_model_set(ValidationRule):
    def validate(self, dpnode: DPnode) -> tuple[bool, str]:
        config = dpnode.get_config()
        if isinstance(config, SensorCfg):
            if config.sensor_type is None or config.sensor_type == api.SENSOR_TYPE.NOT_SET:
                return False, (
                    f"Sensor type not set in {config.description}"
                )
            if config.sensor_model is None or config.sensor_model == root_cfg.FAILED_TO_LOAD:
                return False, (
                    f"Sensor model not set in {config.sensor_type} {config}"
                )
        return True, ""

# Rule 3: no _ in any stream type_id
class Rule3_no_underscore_in_type_id(ValidationRule):
    def validate(self, dpnode: DPnode) -> tuple[bool, str]:
        outputs = dpnode.get_config().outputs
        if outputs:
            for stream in outputs:
                if "_" in stream.type_id:
                    return False, (
                        f"Underscore found in type_id {stream.type_id} in {dpnode}. "
                        f"type_id must not contain underscores."
                    )
        return True, ""

# Rule4: check that cloud_container is set on all datastreams other than type log / df / csv
class Rule4_cloud_container_specified(ValidationRule):
    def validate(self, dpnode: DPnode) -> tuple[bool, str]:
        outputs = dpnode.get_config().outputs
        if outputs:
            for stream in outputs:
                if stream.format not in api.DATA_FORMATS:
                    if (stream.cloud_container is None or 
                        len(stream.cloud_container) < 2 or
                        stream.cloud_container == root_cfg.FAILED_TO_LOAD):
                        return False, (
                            f"cloud_container not set in {dpnode}"
                        )
        return True, ""

# Rule5: check that all cloud_containers exist in the blobstore using cloud_connector.container_exists()
class Rule5_cloud_container_exists(ValidationRule):
    def validate(self, dpnode: DPnode) -> tuple[bool, str]:
        cc = CloudConnector.get_instance(root_cfg.CLOUD_TYPE)
        outputs = dpnode.get_config().outputs
        if outputs:
            for stream in outputs:
                if stream.format not in api.DATA_FORMATS:
                    # Check the Datastream's cloud_container exists
                    if (stream.cloud_container is not None and 
                        not cc.container_exists(stream.cloud_container)):
                        return False, (
                            f"cloud_container {stream.cloud_container} does not exist in "
                            f"{dpnode}"
                        )
        return True, ""

# Rule 6: any datastream of type log, csv or df must have output fields set
class Rule6_csv_output_fields(ValidationRule):
    def validate(self, dpnode: DPnode) -> tuple[bool, str]:
        outputs = dpnode.get_config().outputs
        if outputs:
            for stream in outputs:
                if stream.format in api.DATA_FORMATS:
                    if stream.fields is None or len(stream.fields) == 0:
                        return False, (
                            f"output fields not set in {dpnode} for {stream.type_id}"
                        )
        return True, ""

# Rule 7: don't declare field names that are in use for record_id fields
class Rule7_reserved_fieldnames(ValidationRule):
    def validate(self, dpnode: DPnode) -> tuple[bool, str]:
        outputs = dpnode.get_config().outputs
        if outputs:
            for stream in outputs:
                fields = stream.fields
                if fields is not None:
                    for field in fields:
                        if field in api.ALL_RECORD_ID_FIELDS:
                            return False, (
                                f"output field {field} is reserved in {dpnode} for {stream.type_id}"
                            )
        return True, ""


RULE_SET: list[ValidationRule] = [
    Rule1_outputs_not_empty(),
    Rule2_sensor_type_model_set(),
    Rule3_no_underscore_in_type_id(),
    Rule4_cloud_container_specified(),
    Rule5_cloud_container_exists(),
    Rule6_csv_output_fields(),
    Rule7_reserved_fieldnames(),
]

def validate_trees(dptrees: list[DPtree]) -> tuple[bool, list[str]]:
    """
    Validate the configuration using all added rules.

    Args:
        config (dict): The configuration to validate.

    Returns:
        tuple: (bool, list) where the boolean indicates overall success (True) or failure (False),
                and the list contains error messages for all failed rules.
    """
    is_valid = True
    errors = []

    if not dptrees:
        return False, ["No tree provided for validation."]

    if isinstance(dptrees, DPtree):
        dptrees = [dptrees]    

    #######################################################################################################
    # Run cross-tree validation rules
    #######################################################################################################
    # Build an index of all sensor_type+sensor_index combinations and check for duplicates
    sensor_index_map: dict[str, DPtree] = {}
    for dptree in dptrees:
        config = dptree.sensor.get_config()
        assert isinstance(config, SensorCfg)
        sensor_cfg: SensorCfg = config
        sensor_type_index = f"{sensor_cfg.sensor_type}_{sensor_cfg.sensor_index}"
        if sensor_type_index in sensor_index_map:
            is_valid = False
            errors.append(
                f"Duplicate sensor type and index found ({sensor_cfg.sensor_type} {sensor_cfg.sensor_index})"
                f" in {dptree} and {sensor_index_map[sensor_type_index]}."
                f" Each sensor_type+index must be unique as they represent physical interfaces."
            )
            break
        else:
            # Add the sensor type and index to the map
            sensor_index_map[sensor_type_index] = dptree

    ######################################################################################################
    # Run within-tree validation rules
    #################################################################################################
    if is_valid:
        for dptree in dptrees:
            for rule in RULE_SET:
                try:
                    for dpnode in dptree._nodes.values():
                        success, error_message = rule.validate(dpnode)
                        if not success:
                            is_valid = False
                            errors.append(error_message)
                except Exception as e:
                    is_valid = False
                    errors.append(
                        f"Error validating rule {rule.__class__.__name__}: {e!s}"
                    )
        
    return is_valid, errors


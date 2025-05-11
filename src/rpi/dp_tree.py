from typing import List, NamedTuple, Tuple

from sensor_core import configuration as root_cfg
from sensor_core.dp_config_objects import Stream
from sensor_core.dp_node import DPnode
from sensor_core.sensor import Sensor

logger = root_cfg.setup_logger("sensor_core")

class Edge(NamedTuple):
    source: DPnode
    sink: DPnode
    stream: Stream
    
###############################################################################################
# DPtree represents the flow of data from the sensor to the cloud.
# - the root node is the sensor
# - the internal nodes are DataProcessors that process the data in some way
# - the edges are the data flow between the nodes
#
# DPtree supports the following methods for building the tree:
# - connect(from, to): connect two nodes in the tree
# - chain(): connects multiple nodes along a single edge
#
# DPtree supports the following methods for traversing the tree:
# - get_processors(): returns a list of all processors in the tree
# - search(): searches the tree for a node with the given attributes
#
# connect(from, to) and chain() are used to build the tree.
# - 'from' accepts either a Sensor or a DataProcessor object.
# - 'to' accepts a DataProcessor object.
#
# The first call to connect() or chain() must supply a Sensor object which
# will create the root node of the tree.
#
# The DPtree is built in a top-down fashion, starting from the root node and working down to the leaf nodes.
# The DPtree creates DPtreeNode objects for each node in the tree.
#################################################################################################
class DPtree:
    """Represents a tree structure for data processing nodes.
    The tree consists of a root node (Sensor) and various child nodes (DataProcessors).

    The tree is used to manage the flow of data from the sensor to the cloud storage.
    The tree supports the following operations:
    - connect: Connects two nodes in the tree.
    - chain: Connects multiple nodes in a single chain.
    - validate: Validates the tree structure to ensure all nodes are connected and valid.
    - get_node: Retrieves a node from the tree by its data_id.
    - get_processors: Retrieves all processor nodes in the tree.
    """
    def __init__(self, sensor: Sensor):
        """
        Initializes a DPtree with a Sensor instance that forms the root of the tree.
        """
        # The sensor is the root node of the tree.
        self.sensor: Sensor = sensor

        # Nodes are stored in a dictionary indexed by data_id
        # The data_id represents the edge between the source and the recipient node.
        self._nodes: dict[str, DPnode] = {"root": sensor}
        self._edges: list[Edge] = []

    def connect(
        self,
        source: Tuple[DPnode, int],
        sink: DPnode,
    ) -> None:
        """
        Connects two nodes in the tree.
        Sensor & DataProcessor objects support an out() method that returns an appropriate tuple.
        The index supplied in the tuple is the index of the output stream from the source node.

        Usage example:
            dp_tree.connect(my_sensor_cfg.out(0), data_processor_cfg)
            dp_tree.connect(my_dp_cfg.out(2), my_dp2_cfg)

        Args:
            output: A tuple where the first element is the source node configuration 
                    (Sensor or DataProcessor), and the second element is an integer representing the 
                    output stream identifier.
            input: The destination node configuration, which must be a DataProcessor.
        """
        src_node, stream_index = source

        stream = src_node.get_stream(stream_index)
        if stream is None:
            raise ValueError(f"Node has no output stream at stream_index {stream_index}, {src_node}.")

        if not self.sensor:
            # New tree
            if not isinstance(src_node, Sensor):
                raise ValueError("The first connect() call must provide a Sensor object "
                                 "for the 'from' field.")
            self.sensor = src_node
        else:
            # The source should already exist in the tree.
            if src_node not in self._nodes.values():
                raise ValueError(f"Source node {src_node} is not yet connected; connect it first")

        data_id = stream.get_data_id(self.sensor.sensor_index)
        if data_id in self._nodes:
            raise ValueError(f"Stream {data_id} is already connected.")

        # Add the sink node to our list of known nodes.
        self._nodes[data_id] = sink

        # Build the tree structure by storing the child node with the output index as the key.
        src_node._dpnode_children[stream_index] = sink
        self._edges.append(Edge(src_node, sink, stream))


    def chain(self, *configs: DPnode) -> None:
        """
        Connects multiple nodes in a single chain.

        Args:
            configs: A sequence of configuration objects to connect in order.
        """
        for i in range(len(configs) - 1):
            self.connect((configs[i], 0), configs[i + 1])  # Default metadata value is 0.

    def get_node(self, data_id: str) -> DPnode:
        """
        Retrieves a node from the tree by its data_id.

        Args:
            data_id: The unique identifier for the node.

        Returns:
            The DPtreeNode object.

        Raises:
            KeyError: If the node with the specified data_id does not exist in the tree.
        """
        return self._nodes[data_id]
    
    def get_edges(self) -> List[Edge]:
        """
        Retrieves all edges in the tree.

        Returns:
            A list of tuples representing the edges in the tree.
        """
        return self._edges

    def get_processors(self) -> List[DPnode]:
        """
        Retrieves all processor nodes in the tree.

        Returns:
            A list of DataProcessor objects representing the processors in the tree.
        """
        return [
            node for node in self._nodes.values() if not isinstance(node, Sensor)
        ]

    def export(self) -> dict:
        """
        Exports the tree structure as a dictionary.

        Returns:
            A dictionary representing the tree structure.
        """
        # DptreeNode.export() is recursive - so start with the root node.
        return self.sensor.export()

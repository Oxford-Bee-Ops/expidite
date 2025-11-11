# Expidite RPI Core Architecture

The `expidite_rpi.core` module provides a flexible, threaded data processing framework for Raspberry Pi sensor applications. The architecture follows a tree-based data flow pattern with proper separation of concerns between data acquisition, processing, and storage.

## Core Components Overview

### 1. **EdgeOrchestrator** (`edge_orchestrator.py`)
**Role**: System-wide coordinator and lifecycle manager

The EdgeOrchestrator is the main orchestration singleton that manages the entire sensor ecosystem:

- **Lifecycle Management**: Starts, stops, and monitors all sensor and data processing threads
- **Configuration Loading**: Loads sensor configurations and creates data processing trees
- **Health Monitoring**: Manages device health monitoring and statistics tracking
- **Error Handling**: Coordinates system-wide error recovery and restarts
- **State Management**: Tracks system status (STOPPED, STARTING, RUNNING, STOPPING)

```python
orchestrator = EdgeOrchestrator.get_instance()
orchestrator.load_config()
orchestrator.start_all()  # Starts all sensors and processors
```

### 2. **Data Processing Tree (DPtree)** (`dp_tree.py`)
**Role**: Defines the data flow topology

The DPtree represents the complete data flow from a sensor through various processing stages:

- **Tree Structure**: Root node (Sensor) → Internal nodes (DataProcessors) → Cloud storage
- **Flexible Topology**: Supports linear chains, branching, and complex processing pipelines
- **Connection Management**: Tracks edges between nodes and data stream routing

```python
tree = DPtree(sensor)
tree.connect((sensor, 0), image_processor)
tree.connect((image_processor, 0), cloud_uploader)
```

### 3. **DPnode** (`dp_node.py`)
**Role**: Base class for all data processing entities

DPnode provides common functionality for both sensors and data processors:

- **Stream Management**: Handles multiple output streams per node
- **Data Logging**: Provides `log()`, `save_data()`, and `save_recording()` methods
- **Configuration**: Manages node-specific configuration and validation
- **Statistics**: Tracks performance metrics and data throughput
- **Cloud Integration**: Handles data upload and storage management

### 4. **Sensor** (`sensor.py`)
**Role**: Data acquisition from hardware sensors

Sensors inherit from both `Thread` and `DPnode`:

- **Threading**: Runs continuously in background threads
- **Hardware Interface**: Abstracts specific sensor communication protocols
- **Review Mode**: Supports manual review/debugging mode
- **Error Recovery**: Handles transient errors with exponential backoff
- **Stream Output**: Produces data streams for downstream processing

```python
class MySensor(Sensor):
    def run(self):
        while self.continue_recording():
            data = self.read_sensor_data()
            self.log(0, data)  # Log to stream 0
```

### 5. **DataProcessor** (`dp.py`)
**Role**: Transform and analyze sensor data

DataProcessors implement custom data transformation logic:

- **Abstract Interface**: Implements `process_data()` for custom processing
- **Input Flexibility**: Handles both DataFrames and file lists
- **Chaining**: Can be connected in sequences for complex processing
- **Output Routing**: Can produce multiple output streams

```python
class ImageAnalyzer(DataProcessor):
    def process_data(self, input_data):
        # Process images, extract features
        results = analyze_images(input_data)
        self.save_data(0, results)  # Save to stream 0
```

### 6. **DPworker** (`dp_worker_thread.py`)
**Role**: Execution engine for data processing trees

DPworker threads execute the data processing pipeline:

- **Thread Management**: One worker thread per DPtree
- **Data Flow**: Orchestrates data movement between processing nodes
- **Scheduling**: Manages timing and execution order of processors
- **Error Handling**: Isolates processing errors and maintains system stability

### 7. **CloudConnector** (`cloud_connector.py`)
**Role**: Cloud storage abstraction layer

Provides unified interface to cloud storage services:

- **Storage Abstraction**: Currently supports Azure Blob Storage
- **Upload Management**: Handles file and data uploads with retry logic
- **Container Management**: Manages cloud storage containers and permissions
- **Performance**: Supports both synchronous and asynchronous upload modes

### 8. **Configuration System** (`configuration.py`, `dp_config_objects.py`)
**Role**: System-wide configuration management

- **Device Configuration**: Hardware-specific settings and capabilities
- **Stream Definitions**: Data format and routing specifications
- **Processing Configuration**: DataProcessor parameters and chains
- **Cloud Settings**: Storage containers, credentials, and upload policies

## Data Flow Architecture

### Basic Flow Pattern
```
Sensor → [DataProcessor₁] → [DataProcessor₂] → Cloud Storage
   ↓           ↓               ↓
 Stream₀   Stream₁        Stream₂
```

### Stream-Based Processing
- **Multiple Streams**: Each node can output multiple data streams
- **Stream Routing**: Streams connect specific outputs to inputs
- **Parallel Processing**: Different streams can be processed independently
- **Format Flexibility**: Streams support files, DataFrames, and structured logs

### Threading Model
- **Sensor Threads**: One thread per sensor for continuous data acquisition
- **Worker Threads**: One DPworker thread per DPtree for processing
- **Isolation**: Thread isolation prevents single sensor failures from affecting others
- **Coordination**: EdgeOrchestrator manages thread lifecycle and communication

## Configuration Patterns

### Stream Definition
```python
@dataclass
class Stream:
    description: str
    type_id: str           # Unique identifier for stream type
    index: int            # Position in node's output array
    format: FORMAT        # Data format (CSV, JSON, MP4, etc.)
    cloud_container: str  # Target cloud storage container
    sample_probability: str  # Sampling rate for data reduction
```

### Node Configuration
```python
@dataclass
class SensorCfg(DPtreeNodeCfg):
    sensor_type: SENSOR_TYPE
    sensor_index: int
    outputs: list[Stream]     # Defines output streams
    description: str
    # Sensor-specific fields...

@dataclass  
class DataProcessorCfg(DPtreeNodeCfg):
    outputs: list[Stream]
    description: str
    # Processor-specific fields...
```

### Tree Building
```python
def create_sensor_tree():
    # Create sensor with configuration
    sensor = RPiCameraSensor(camera_config)
    
    # Build processing tree
    tree = DPtree(sensor)
    tree.connect((sensor, 0), ImageProcessor(proc_config))
    tree.connect((ImageProcessor, 0), CloudUploader(upload_config))
    
    return [tree]
```

## Error Handling & Resilience

### Sensor-Level Recovery
- **Exception Isolation**: Sensor errors don't affect other sensors
- **Retry Logic**: Exponential backoff for transient failures
- **Graceful Degradation**: Continue operation with reduced functionality

### System-Level Recovery
- **Health Monitoring**: DeviceHealth tracks system metrics
- **Automatic Restart**: EdgeOrchestrator can restart failed components
- **Persistent State**: Configuration and processing state survive restarts

### Data Integrity
- **Atomic Operations**: File operations are atomic where possible
- **Validation**: Stream validation ensures data consistency
- **Sampling**: Configurable sampling reduces data volume while preserving coverage

## Usage Patterns

### Simple Sensor Setup
```python
# 1. Define sensor configuration
sensor_cfg = MySensorCfg(
    sensor_type=SENSOR_TYPE.CAMERA,
    sensor_index=0,
    outputs=[main_stream, review_stream]
)

# 2. Create sensor
sensor = MySensor(sensor_cfg)

# 3. Build tree (optional processing)
tree = DPtree(sensor)

# 4. Register with orchestrator
orchestrator = EdgeOrchestrator.get_instance()
orchestrator.dp_trees = [tree]
orchestrator.start_all()
```

### Complex Processing Pipeline
```python
def create_complex_pipeline():
    # Sensor
    camera = RPiCamera(camera_config)
    
    # Processing chain
    tree = DPtree(camera)
    tree.connect((camera, 0), ImageResizer(resize_config))
    tree.connect((ImageResizer, 0), FeatureExtractor(feature_config))
    tree.connect((FeatureExtractor, 0), CloudUploader(upload_config))
    
    # Parallel branch for thumbnails  
    tree.connect((camera, 0), ThumbnailGenerator(thumb_config))
    tree.connect((ThumbnailGenerator, 0), ThumbnailUploader(thumb_upload_config))
    
    return [tree]
```

## Key Design Principles

1. **Modularity**: Clear separation between acquisition, processing, and storage
2. **Extensibility**: Easy to add new sensors and processors
3. **Reliability**: Robust error handling and recovery mechanisms
4. **Performance**: Efficient threading and data flow management
5. **Configuration**: Declarative configuration system for easy deployment
6. **Testing**: Comprehensive testing framework with mocking support

This architecture provides a solid foundation for building complex sensor data collection and processing systems on Raspberry Pi devices, with built-in support for cloud storage, error recovery, and system monitoring.
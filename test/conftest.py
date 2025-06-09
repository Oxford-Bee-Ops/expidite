import pytest
from time import sleep
from expidite_rpi.core import configuration as root_cfg
from expidite_rpi.core.cloud_connector import CloudConnector

@pytest.fixture(autouse=True)
def shutdown_cloud_connector():
    yield
    # Ensure CloudConnector is properly shut down after each test
    try:
        cc = CloudConnector.get_instance(root_cfg.CloudType.AZURE)
        cc.shutdown()
        sleep(1)
    except Exception:
        # Ignore errors if connector was never created
        pass

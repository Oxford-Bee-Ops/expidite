import time

import pytest
from expidite_rpi.core import configuration as root_cfg
from expidite_rpi.core.cloud_connector import CloudConnector

# Set up logger for test execution
test_logger = root_cfg.setup_logger("expidite")


@pytest.fixture(autouse=True)
def log_test_lifecycle(request):
    """
    Pytest fixture that automatically logs the start and end of every test.
    This runs for all tests without requiring a decorator.
    """
    test_name = request.node.name
    module_name = request.node.module.__name__ if request.node.module else "unknown"
    
    # Log test start
    start_time = time.time()
    test_logger.info(f"[PYTEST START] {module_name}::{test_name}")
    
    yield  # This is where the test runs
    
    # Log test end
    duration = time.time() - start_time
    test_result = "PASSED" if not request.node.rep_call.failed else "FAILED"
    
    if test_result == "PASSED":
        test_logger.info(f"[PYTEST END] {module_name}::{test_name} - "
                         f"{test_result} (Duration: {duration:.3f}s)")
    else:
        test_logger.error(f"[PYTEST END] {module_name}::{test_name} - "
                          f"{test_result} (Duration: {duration:.3f}s)")


@pytest.hookimpl(tryfirst=True, hookwrapper=True)
def pytest_runtest_makereport(item, call):
    """
    Hook to capture test results for the log_test_lifecycle fixture.
    """
    outcome = yield
    rep = outcome.get_result()
    setattr(item, f"rep_{rep.when}", rep)

@pytest.fixture(autouse=True)
def shutdown_cloud_connector():
    yield
    # Ensure CloudConnector is properly shut down after each test
    try:
        cc = CloudConnector.get_instance(root_cfg.CloudType.AZURE)
        cc.shutdown()
        time.sleep(1)
    except Exception:
        # Ignore errors if connector was never created
        pass

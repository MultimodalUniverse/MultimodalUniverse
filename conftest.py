import os


def pytest_configure(config):
    # Register additional markers to run dataset tests selectively
    for dir in os.listdir("scripts"):
        config.addinivalue_line(
            "markers", f"{dir}: mark test to run only {dir} dataset tests."
        )

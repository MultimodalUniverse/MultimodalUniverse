import pytest
import os


def pytest_addoption(parser: pytest.Parser):
    parser.addoption(
        "--diff",
        dest="diff",
        nargs="*",
        help="Modified files to be considered to skip the tests.",
    )


def pytest_configure(config: pytest.Config):
    # Register additional markers to run dataset tests selectively
    config.addinivalue_line(
        "markers", "dataset(name): mark test to run only dataset tests."
    )


def pytest_runtest_setup(item: pytest.Item):
    dataset_mark = [mark.args[0] for mark in item.iter_markers(name="dataset")]
    modified_files = item.config.getoption("--diff")
    if dataset_mark and modified_files:
        assert (
            len(dataset_mark) == 1
        ), f"Test should have only one dataset marker but got {dataset_mark}"
        dataset_mark = dataset_mark[0]
        modified_datasets = set(
            os.path.basename(os.path.dirname(path))
            for path in modified_files
            if "scripts" in path
        )
        if dataset_mark not in modified_datasets:
            pytest.skip("Dataset has not been modified.")

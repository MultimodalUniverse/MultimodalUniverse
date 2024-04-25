import inspect
import os.path as osp

import pytest


def mark_dataset_test() -> pytest.MarkDecorator:
    dataset_name = osp.basename(osp.dirname(inspect.stack()[1].filename))
    return pytest.mark.dataset(dataset_name)

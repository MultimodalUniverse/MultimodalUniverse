import inspect
import os.path as osp
import pytest


def mark_dataset_test() -> pytest.MarkDecorator:
    mark_name = osp.basename(osp.dirname(inspect.stack()[1].filename))
    return getattr(pytest.mark, mark_name)

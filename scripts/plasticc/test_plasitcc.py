import os.path as osp
from unittest import TestCase

from datasets import load_dataset

from astropile.tests import mark_dataset_test
from .build_parent_sample import main as build


@mark_dataset_test()
class TestPlasticc(TestCase):
    def test_process(self):
        class BuildInput:
            plasticc_data_path = "."
            output_path = "."
            num_processes = 1
            tiny = True

        build(BuildInput)

        dataset = load_dataset(
            osp.abspath(f"{osp.dirname(__file__)}/plasticc.py"),
            trust_remote_code=True,
            split="train",
        )
        self.assertTrue(len(dataset) > 0)
        self.assertTrue(next(iter(dataset)))

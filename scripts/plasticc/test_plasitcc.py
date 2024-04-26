import os.path as osp
from unittest import TestCase

from datasets import load_dataset

from astropile.tests import mark_dataset_test
from build_parent_sample import PlasticcDataProcessor


DIR = osp.abspath(osp.dirname(__file__))


@mark_dataset_test()
class TestPlasticc(TestCase):
    def test_process(self):
        data_processor = PlasticcDataProcessor(DIR, DIR, tiny=True)
        data_processor.main()

        dataset = load_dataset(
            osp.abspath(f"{osp.dirname(__file__)}/plasticc.py"),
            trust_remote_code=True,
            split="train",
        )
        self.assertTrue(len(dataset) > 0)
        self.assertTrue(next(iter(dataset)))

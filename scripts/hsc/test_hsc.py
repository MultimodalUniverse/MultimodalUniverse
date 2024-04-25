import os.path as osp
from unittest import TestCase

from datasets import load_dataset

from astropile.tests import mark_dataset_test
from .build_parent_sample import _image_size, main


DATA_DIR = osp.abspath(f"{osp.dirname(__file__)}")
DATA_SCRIPT_PATH = osp.join(f"{DATA_DIR}/hsc.py")
PDR3_DUD_DB = osp.join(f"{DATA_DIR}/pdr_3_dud_22.5.sql")
PDR3_WIDE_DB = osp.join(f"{DATA_DIR}/pdr_3_wide_22.5.sql")


class ArgInput:
    query_file = PDR3_DUD_DB
    output_dir = "."
    rerun = "pdr3_dud_dev"
    num_processes = 1
    tiny = True
    dr = "pdr3"
    temp_dir = "/tmp"
    cutout_size = _image_size


@mark_dataset_test()
class TestHSC(TestCase):
    def test_dud(self):
        main(ArgInput())
        dset = load_dataset(
            DATA_SCRIPT_PATH, PDR3_DUD_DB, trust_remote_code=True, split="train"
        ).with_format("numpy")
        batch = next(iter(dset))
        self.assertTrue(batch)

    def test_wide(self):
        input = ArgInput()
        input.query_file = PDR3_WIDE_DB
        input.rerun = "pdr3_wide"
        main(input)
        dset = load_dataset(
            DATA_SCRIPT_PATH, PDR3_WIDE_DB, trust_remote_code=True, split="train"
        ).with_format("numpy")
        batch = next(iter(dset))
        self.assertTrue(batch)

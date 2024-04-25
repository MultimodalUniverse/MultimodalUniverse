from unittest import TestCase

from build_parent_sample import _image_size, main
from datasets import load_dataset

from astropile.tests import mark_dataset_test


class ArgInput:
    query_file = "pdr_3_dud_22.5.sql"
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
            "./hsc.py", "pdr3_dud_22.5", trust_remote_code=True, split="train"
        ).with_format("numpy")
        batch = next(iter(dset))
        self.assertTrue(batch)

    def test_wide(self):
        input = ArgInput()
        input.query_file = "pdr3_wide_22.5.sql"
        input.rerun = "pdr3_wide"
        main(input)
        dset = load_dataset(
            "./hsc.py", "pdr3_wide_22.5", trust_remote_code=True, split="train"
        ).with_format("numpy")
        batch = next(iter(dset))
        self.assertTrue(batch)

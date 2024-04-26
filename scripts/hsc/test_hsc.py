import os
import os.path as osp
import pytest
from unittest import TestCase

from datasets import load_dataset

from astropile.tests import mark_dataset_test
from build_parent_sample import HSCDataProcessor


DATA_DIR = osp.abspath(osp.dirname(__file__))
DATA_SCRIPT_PATH = osp.join(DATA_DIR, "hsc.py")
PDR3_DUD_DB = osp.join(DATA_DIR, "pdr_3_dud_22.5.sql")
PDR3_WIDE_DB = osp.join(DATA_DIR, "pdr_3_wide_22.5.sql")


def skip_not_set() -> bool:
    return "SSP_PDR_PWD" not in os.environ


@mark_dataset_test()
@pytest.mark.skipif(skip_not_set(), reason="Credentials for SSP PDR not available.")
class TestHSC(TestCase):
    def test_dud(self):
        data_processor = HSCDataProcessor(
            DATA_DIR, PDR3_DUD_DB, "pdr3", "pdr3_dud_rev", True
        )
        data_processor.main()
        dset = load_dataset(
            DATA_SCRIPT_PATH, PDR3_DUD_DB, trust_remote_code=True, split="train"
        ).with_format("numpy")
        batch = next(iter(dset))
        self.assertTrue(batch)

    def test_wide(self):
        data_processor = HSCDataProcessor(
            DATA_DIR, PDR3_WIDE_DB, "pdr3", "pdr3_wide", True
        )
        data_processor.main()
        dset = load_dataset(
            DATA_SCRIPT_PATH, PDR3_WIDE_DB, trust_remote_code=True, split="train"
        ).with_format("numpy")
        batch = next(iter(dset))
        self.assertTrue(batch)

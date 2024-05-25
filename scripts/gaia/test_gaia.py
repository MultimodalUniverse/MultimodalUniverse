import os.path as osp
import sys

sys.path.append(osp.dirname(osp.realpath(__file__)))

from unittest import TestCase

import datasets

from astropile.tests import mark_dataset_test
from download_parts import main as download
from healpixify import main as healpixify
from merge_parts import main as merge

DIR = osp.abspath(osp.dirname(__file__))

@mark_dataset_test()
class TestGaiaProcess(TestCase):
    def test_process(self):
        class DowlonadInput:
            aria2 = False
            tiny = True
            output_dir = DIR

        download(DowlonadInput)

        class MergInput:
            input_dir = DIR
            output_file = osp.join(DIR, "merged.hdf5")

        merge(MergInput)

        class HealixipyInput:
            input_file = osp.join(DIR, "merged.hdf5")
            output_dir = DIR
            nside = 8
            num_procs = 10

        healpixify(HealixipyInput)


@mark_dataset_test()
class TestGaiaLoad(TestCase):
    def setUp(self):
        super().setUp()
        self.ds = datasets.load_dataset(
            osp.abspath(f"{osp.dirname(__file__)}/gaia.py"),
            trust_remote_code=True,
            split="train",
            streaming=True,
        ).with_format("numpy")

    def test_load(self):
        batch = next(iter(self.ds))
        self.assertTrue(batch)

    def test_spectral_coefficients(self):
        batch = next(iter(self.ds))
        self.assertEqual(batch["spectral_coefficients"]["coeff"].size, 110)
        self.assertEqual(batch["spectral_coefficients"]["coeff_error"].size, 110)

    def test_astrometry(self):
        batch = next(iter(self.ds))
        for k in ["ra", "dec", "pmra", "pmdec", "parallax"]:
            self.assertTrue(k in batch["astrometry"].keys())

    def test_photometry(self):
        batch = next(iter(self.ds))
        for k in ["bp_rp", "phot_g_mean_mag", "phot_bp_mean_mag", "phot_rp_mean_mag"]:
            self.assertTrue(k in batch["photometry"].keys())

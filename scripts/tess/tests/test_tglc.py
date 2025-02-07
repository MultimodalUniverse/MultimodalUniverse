import os
import h5py 
from datasets import load_dataset, Dataset
from datasets.data_files import DataFilesPatternsDict

from . import PIPELINES, _TINY_SIZE 

tglc = PIPELINES["tglc"]

def test_sh_download_script():
    '''Check the .sh file has been downloaded correctly.'''
    assert os.path.exists(tglc["SH_FP"]), f"Expected {tglc['SH_FP']} to exist"

def test_check_target_csv_file():
    '''Check the sector CSV file is downloaded correctly.'''
    assert os.path.exists(tglc["CSV_FP"]), f"Expected {tglc['CSV_FP']} to exist"
    assert os.path.getsize(tglc["CSV_FP"]) == tglc["CSV_FILE_SIZE"], f"Expected {tglc['CSV_FILE_SIZE']} bytes, but found {os.path.getsize(tglc['CSV_FP'])}"

def test_create_sector_catalog():
    assert os.path.exists(tglc["CATALOG_FP"])
    
def test_convert_fits_to_standard_format():
    # Process fits to standard format
    assert os.path.exists(tglc["HDF5_FP"]), f"Expected {tglc['HDF5_FP']} to exist"
    assert os.path.exists(tglc["TEST_HDF5_FP"]), f"Expected {tglc['TEST_HDF5_FP']} to exist"
    with h5py.File(tglc["TEST_HDF5_FP"], 'r') as f:
        keys_ = list(f.keys())
        assert keys_.sort(key=len) == ['DEC', 'RA', 'TIC_ID', 'aper_flux', 'aper_flux_err', 'GAIADR3_ID', 'psf_flux', 'psf_flux_err', 'tess_flags', 'tglc_flags', 'time'].sort(key=len), f"Expected keys to be ['lightcurve', 'RA', 'DEC', 'TIC_ID', 'GAIADR3_ID', 'psf_flux_err', 'aper_flux_err'] in {tglc['TEST_HDF5_FP']}. Got {keys_}."

def test_dataloader():
    tglc_loader = load_dataset(
        tglc["LOADING_SCRIPT_FP"], 
        trust_remote_code=True, 
        data_files = DataFilesPatternsDict.from_patterns(
            {"train": ["./tests/tiny_tglc/TGLC/healpix=*/*.hdf5"]
        })
    )
    dset = tglc_loader.with_format('numpy')['train']

    assert isinstance(dset, Dataset), f"Expected dset to be a datasets.Dataset, got {type(dset)}"
    assert dset.num_rows == _TINY_SIZE, f"Expected {_TINY_SIZE} rows, got {dset.num_rows}"
    assert dset.features == tglc["FEATURES"], f"Expected features to be {tglc['FEATURES']}, got {dset.features}"
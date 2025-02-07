import os
import h5py 
from datasets import load_dataset, Dataset
from datasets.data_files import DataFilesPatternsDict
from datasets import Value, Sequence

from . import PIPELINES, _TINY_SIZE

spoc = PIPELINES["spoc"]

def test_sh_download_script():
    '''Check the .sh file has been downloaded correctly.'''
    assert os.path.exists(spoc["SH_FP"]), f"Expected {spoc['SH_FP']} to exist"

def test_check_target_csv_file():
    '''Check the sector CSV file is downloaded correctly.'''
    assert os.path.exists(spoc["CSV_FP"]), f"Expected {spoc['CSV_FP']} to exist"
    assert os.path.getsize(spoc["CSV_FP"]) == spoc["CSV_FILE_SIZE"], f"Expected {spoc['CSV_FILE_SIZE']} bytes, but found {os.path.getsize(spoc['CSV_FP'])}"

def test_convert_fits_to_standard_format():
    # Process fits to standard format
    assert os.path.exists(spoc["HDF5_FP"]), f"Expected {spoc['HDF5_FP']} to exist"
    assert os.path.exists(spoc["TEST_HDF5_FP"]), f"Expected {spoc['TEST_HDF5_FP']} to exist"
    with h5py.File(spoc["TEST_HDF5_FP"], 'r') as f:
        keys_ = list(f.keys())
        assert keys_.sort(key=len) == ['DEC', 'RA', 'TIC_ID', 'kspsap_flux', 'kspsap_flux_err', 'kspsap_flux_lag', 'kspsap_flux_sml', 'logg', 'mh', 'orbitid', 'quality', 'radius', 'sap_bkg', 'sap_bkg_err', 'sap_flux', 'sap_x', 'sap_y', 'teff', 'tess_mag', 'time'].sort(key=len) , f"Expected keys to be ['DEC', 'RA', 'TIC_ID', 'kspsap_flux', 'kspsap_flux_err', 'kspsap_flux_lag', 'kspsap_flux_sml', 'logg', 'mh', 'orbitid', 'quality', 'radius', 'sap_bkg', 'sap_bkg_err', 'sap_flux', 'sap_x', 'sap_y', 'teff', 'tess_mag', 'time'], got {keys_}."

def test_create_sector_catalog():
    assert os.path.exists(spoc["CATALOG_FP"])

def test_dataloader():
    spoc_loader = load_dataset(
        spoc["LOADING_SCRIPT_FP"], 
        trust_remote_code=True, 
        data_files = DataFilesPatternsDict.from_patterns(
            {"train": ["./tests/tiny_spoc/SPOC/healpix=*/*.hdf5"]}
        )
    )
    dset = spoc_loader.with_format('numpy')['train']
    assert isinstance(dset, Dataset), f"Expected dset to be a datasets.Dataset, got {type(dset)}"
    assert dset.num_rows == _TINY_SIZE, f"Expected {_TINY_SIZE} rows, got {dset.num_rows}"
    assert dset.features == spoc["FEATURES"], f"Expected features to be {spoc['FEATURES']}, got {dset.spoc['FEATURES']}"

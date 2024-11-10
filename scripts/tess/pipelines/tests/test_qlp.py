import os
import h5py 
from datasets import load_dataset, Dataset
from datasets.data_files import DataFilesPatternsDict

from __init__ import PIPELINES, _TINY_SIZE

qlp = PIPELINES["qlp"]

def test_sh_download_script():
    '''Check the .sh file has been downloaded correctly.'''
    assert os.path.exists(qlp["SH_FP"]), f"Expected {qlp["SH_FP"]} to exist"
    assert os.path.getsize(qlp["SH_FP"]) == qlp["SH_FILE_SIZE"], f"Expected {qlp["SH_FILE_SIZE"]} bytes, but found {os.path.getsize(qlp["SH_FP"])}"

def test_check_target_csv_file():
    '''Check the sector CSV file is downloaded correctly.'''
    assert os.path.exists(qlp["CSV_FP"]), f"Expected {qlp["CSV_FP"]} to exist"
    assert os.path.getsize(qlp["CSV_FP"]) == qlp["CSV_FILE_SIZE"], f"Expected {qlp["CSV_FILE_SIZE"]} bytes, but found {os.path.getsize(qlp["CSV_FP"])}"

def test_create_sector_catalog():
    assert os.path.exists(qlp["CATALOG_FP"])
 
def test_batched_download():
    # Download the fits light curves using the sector catalog
    assert os.path.exists(qlp["FITS_DIR"]), f"Expected {qlp["FITS_DIR"]} to exist"

    n_files = 0
    for _, _, files in os.walk(qlp["FITS_DIR"]):
        n_files += len([f for f in files if f.endswith('.fits')])
    assert n_files == _TINY_SIZE, f"Expected {_TINY_SIZE} .fits files in {qlp["FITS_DIR"]}, but found {n_files}"
    
def test_convert_fits_to_standard_format():
    # Process fits to standard format
    assert os.path.exists(qlp["HDF5_FP"]), f"Expected {qlp["HDF5_FP"]} to exist"
    assert os.path.exists(qlp["TEST_HDF5_FP"]), f"Expected {qlp["TEST_HDF5_FP"]} to exist"
    with h5py.File(qlp["TEST_HDF5_FP"], 'r') as f:
        keys_ = list(f.keys())
        assert keys_.sort(key=len) == ['DEC', 'RA', 'TIC_ID', 'kspsap_flux', 'kspsap_flux_err', 'kspsap_flux_lag', 'kspsap_flux_sml', 'logg', 'mh', 'orbitid', 'quality', 'radius', 'sap_bkg', 'sap_bkg_err', 'sap_flux', 'sap_x', 'sap_y', 'teff', 'tess_mag', 'time'].sort(key=len) , f"Expected keys to be {"['DEC', 'RA', 'TIC_ID', 'kspsap_flux', 'kspsap_flux_err', 'kspsap_flux_lag', 'kspsap_flux_sml', 'logg', 'mh', 'orbitid', 'quality', 'radius', 'sap_bkg', 'sap_bkg_err', 'sap_flux', 'sap_x', 'sap_y', 'teff', 'tess_mag', 'time']"}, got {keys_}."

def test_dataloader():
    qlp_loader = load_dataset(
        qlp["LOADING_SCRIPT_FP"], 
        trust_remote_code=True, 
        data_files = DataFilesPatternsDict.from_patterns({"train": ["./tiny_qlp/QLP/healpix=*/*.hdf5"]})
    )
    dset = qlp_loader.with_format('numpy')['train']

    assert isinstance(dset, Dataset), f"Expected dset to be a datasets.Dataset, got {type(dset)}"
    assert dset.num_rows == _TINY_SIZE, f"Expected {_TINY_SIZE} rows, got {dset.num_rows}"
    assert dset.features == qlp["FEATURES"], f"Expected features to be {qlp['FEATURES']}, got {dset.qlp['FEATURES']}"
import os
import sys 
sys.path.append("..") 

from spoc import SPOC_Downloader
from qlp import QLP_Downloader
from tglc import TGLC_Downloader

from datasets import Value, Sequence

N_PROCESSES = 4 
SECTOR = 23
SECTOR_STR = f"s0023"
_TINY_SIZE = 100

PIPELINES = {
    'qlp': {
        "DATA_PATH": "./tiny_qlp", 
        "FITS_DIR":  os.path.join("./tiny_qlp", 'fits'), 
        "Downloader": QLP_Downloader,
        "SH_FILE_SIZE": 84116620,
        "CSV_FILE_SIZE": 13911657,
        "SH_FP": f"./tiny_qlp/{SECTOR_STR}_fits_download_script.sh",
        "CSV_FP": f"./tiny_qlp/{SECTOR_STR}_target_list.csv",
        "CATALOG_FP": f"./tiny_qlp/{SECTOR_STR}_catalog_tiny.hdf5",
        "HDF5_FP": f"./tiny_qlp/QLP",
        "TEST_HDF5_FP": f"./tiny_qlp/QLP/healpix=569/001-of-001.hdf5",
        "FEATURES": {
            "lightcurve" : Sequence({
                'time': Value(dtype="float32"),
                'sap_flux': Value(dtype="float32"),
                'kspsap_flux':  Value(dtype="float32"),
                'kspsap_flux_err':  Value(dtype="float32"),
                'quality':  Value(dtype="float32"), 
                'orbitid':  Value(dtype="float32"),
                'sap_x':  Value(dtype="float32"),
                'sap_y':  Value(dtype="float32"),
                'sap_bkg':  Value(dtype="float32"),
                'sap_bkg_err':  Value(dtype="float32"),
                'kspsap_flux_sml':  Value(dtype="float32"),
                'kspsap_flux_lag':  Value(dtype="float32"),
            }),
            'RA':  Value(dtype="float32"),
            'DEC':  Value(dtype="float32"),
            'TIC_ID': Value(dtype="string"),
            'tess_mag': Value(dtype="float32"),
            'radius':  Value(dtype="float32"),
            'teff': Value(dtype="float32"),
            'logg': Value(dtype="float32"),
            'mh': Value(dtype="float32")
        },
        "LOADING_SCRIPT_FP": "../qlp.py"
    }, 
    'spoc': {
        "DATA_PATH": "./tiny_spoc", 
        "FITS_DIR":  os.path.join("./tiny_spoc", 'fits'), 
        "Downloader": SPOC_Downloader,
        "SH_FILE_SIZE": 47129525,
        "CSV_FILE_SIZE": 7844066,
        "SH_FP": f"./tiny_spoc/{SECTOR_STR}_fits_download_script.sh",
        "CSV_FP": f"./tiny_spoc/{SECTOR_STR}_target_list.csv",
        "CATALOG_FP": f"./tiny_spoc/{SECTOR_STR}_catalog_tiny.hdf5",
        "HDF5_FP": f"./tiny_spoc/SPOC",
        "TEST_HDF5_FP": f'./tiny_spoc/SPOC/healpix=620/001-of-001.hdf5',
        "FEATURES": {
            'lightcurve' : Sequence({
                'time': Value(dtype="float32"),
                'flux': Value(dtype="float32"),
                'flux_err':  Value(dtype="float32"),
                'quality':  Value(dtype="float32")
            }),
            'RA':  Value(dtype="float32"),
            'DEC':  Value(dtype="float32"),
            'TIC_ID': Value(dtype="string")
        },
        "LOADING_SCRIPT_FP": "../spoc.py"
    }, 
    'tglc': {
        "DATA_PATH": "./tiny_tglc", 
        "FITS_DIR":  os.path.join("./tiny_tglc", 'fits'), 
        "Downloader": TGLC_Downloader,
        "SH_FILE_SIZE": 441048360,
        "CSV_FILE_SIZE": 95965988,
        "SH_FP": f"./tiny_tglc/{SECTOR_STR}_fits_download_script.sh",
        "CSV_FP": f"./tiny_tglc/{SECTOR_STR}_target_list.csv",
        "CATALOG_FP": f"./tiny_tglc/{SECTOR_STR}_catalog_tiny.hdf5",
        "HDF5_FP": f"./tiny_tglc/TGLC",
        "TEST_HDF5_FP": f"./tiny_tglc/TGLC/healpix=544/001-of-001.hdf5",
        "FEATURES": {
            "lightcurve" : Sequence({
                'time': Value(dtype="float32"),
                'psf_flux': Value(dtype="float32"),
                'aper_flux':  Value(dtype="float32"),
                'tess_flags':  Value(dtype="float32"),
                'tglc_flags':  Value(dtype="float32"), 
            }),
            'RA':  Value(dtype="float32"),
            'DEC':  Value(dtype="float32"),
            'TIC_ID': Value(dtype="string"),
            'GAIADR3_ID': Value(dtype="string"),
            'aper_flux_err':  Value(dtype="float32"),
            'psf_flux_err': Value(dtype="float32"),
        },
        "LOADING_SCRIPT_FP": "../tglc.py",
    }
}



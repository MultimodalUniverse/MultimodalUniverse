import os
import pytest
import h5py 
from datasets import load_dataset, Dataset
from datasets.data_files import DataFilesPatternsDict
from datasets import Value, Sequence
from qlp import QLP_Downloader

DATA_PATH = "../tiny_qlp"
FITS_DIR = os.path.join(DATA_PATH, f'fits')

N_PROCESSES = 4 
SECTOR = 23

SH_FILE_SIZE = 84116620 # os.path.getsize(fp)
CSV_FILE_SIZE = 13911657

_TINY_SIZE = 100

FEATURES = {
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
}

DOWNLOADER = QLP_Downloader(
        sector=SECTOR,
        data_path = DATA_PATH, 
        hdf5_output_dir = DATA_PATH,
        fits_dir = FITS_DIR,
        n_processes = N_PROCESSES
)

sh_fp = f'{DATA_PATH}/{DOWNLOADER.sector_str}_fits_download_script.sh'
csv_fp = f'{DATA_PATH}/{DOWNLOADER.sector_str}_target_list.csv'
catalog_fp = f'{DATA_PATH}/{DOWNLOADER.sector_str}_catalog_tiny.hdf5'
hdf5_fp = f'{DATA_PATH}/QLP'
test_hdf5_fp = f'{DATA_PATH}/QLP/healpix=569/001-of-001.hdf5'

def test_sh_download_script():
    '''Check the .sh file has been downloaded correctly.'''
    DOWNLOADER.download_sh_script()
    assert os.path.exists(sh_fp), f"Expected {sh_fp} to exist"
    assert os.path.getsize(sh_fp) == SH_FILE_SIZE, f"Expected {SH_FILE_SIZE} bytes, but found {os.path.getsize(sh_fp)}"

def test_check_target_csv_file():
    '''Check the sector CSV file is downloaded correctly.'''
    DOWNLOADER.download_target_csv_file()
    assert os.path.exists(csv_fp), f"Expected {csv_fp} to exist"
    assert os.path.getsize(csv_fp) == CSV_FILE_SIZE, f"Expected {CSV_FILE_SIZE} bytes, but found {os.path.getsize(csv_fp)}"

@pytest.fixture
def create_sector_catalog():
    '''Create the sector catalog.'''
    return DOWNLOADER.create_sector_catalog(save_catalog = True, tiny = True)[:_TINY_SIZE]

def test_create_sector_catalog():
    assert os.path.exists(catalog_fp)
 
@pytest.fixture
def download_sector_catalog(create_sector_catalog):
    return DOWNLOADER.batched_download(create_sector_catalog, tiny=True)

def test_batched_download():
    # Download the fits light curves using the sector catalog
    assert os.path.exists(FITS_DIR), f"Expected {FITS_DIR} to exist"

    n_files = 0
    for _, _, files in os.walk(FITS_DIR):
        n_files += len([f for f in files if f.endswith('.fits')])
    assert n_files == _TINY_SIZE, f"Expected {_TINY_SIZE} .fits files in {FITS_DIR}, but found {n_files}"
    
def test_convert_fits_to_standard_format(create_sector_catalog):
    # Process fits to standard format
    DOWNLOADER.convert_fits_to_standard_format(create_sector_catalog)
    assert os.path.exists(hdf5_fp), f"Expected {hdf5_fp} to exist"
    assert os.path.exists(test_hdf5_fp), f"Expected {test_hdf5_fp} to exist"
    with h5py.File(test_hdf5_fp, 'r') as f:
        keys_ = list(f.keys())
        assert keys_.sort(key=len) == ['DEC', 'RA', 'TIC_ID', 'kspsap_flux', 'kspsap_flux_err', 'kspsap_flux_lag', 'kspsap_flux_sml', 'logg', 'mh', 'orbitid', 'quality', 'radius', 'sap_bkg', 'sap_bkg_err', 'sap_flux', 'sap_x', 'sap_y', 'teff', 'tess_mag', 'time'].sort(key=len) , f"Expected keys to be {"['DEC', 'RA', 'TIC_ID', 'kspsap_flux', 'kspsap_flux_err', 'kspsap_flux_lag', 'kspsap_flux_sml', 'logg', 'mh', 'orbitid', 'quality', 'radius', 'sap_bkg', 'sap_bkg_err', 'sap_flux', 'sap_x', 'sap_y', 'teff', 'tess_mag', 'time']"}, got {keys_}."

def test_dataloader():
    tglc = load_dataset(
        "../qlp.py", 
        trust_remote_code=True, 
        data_files = DataFilesPatternsDict.from_patterns({"train": ["./tiny_qlp/QLP/healpix=*/*.hdf5"]})
    )
    dset = tglc.with_format('numpy')['train']

    assert isinstance(dset, Dataset), f"Expected dset to be a datasets.Dataset, got {type(dset)}"
    assert dset.num_rows == _TINY_SIZE, f"Expected {_TINY_SIZE} rows, got {dset.num_rows}"
    assert dset.features == FEATURES, f"Expected features to be {FEATURES}, got {dset.features}"
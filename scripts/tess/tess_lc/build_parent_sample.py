import os 
from astropy.io import fits
import numpy as np
from astropy.table import Table, join
import h5py
import healpy as hp
from multiprocessing import Pool
from tqdm import tqdm
import subprocess
from datasets import load_dataset

_healpix_nside = 16

PIPELINES = ['TGLC']

# To-Do:
# - Cross-matching across sectors with gaia_id 

def read_sh(fp: str):
    '''
    read_sh reads an sh file, parses and returns the curls commands from the file
    
    Parameters
    ----------
    fp: str, path to the .sh file
    
    Returns
    ------- 
    lines: list, list of curl commands in the .sh file for downloading a single light curve
    '''

    with open(fp, 'r') as f:
        lines = f.readlines()[1:]
    return lines

def parse_line(line: str):
    '''
    Parse a line from the .sh file, extract the relevant parts (gaia_id, cam, ccd, fp1, fp2, fp3, fp4) and return them as a list

    Parameters
    ----------
    line: str, a line from the .sh file
    
    Returns
    ------- 
    params: list, list of parameters extracted from the line
    '''

    parts = line.split()
    output_path = parts[4].strip("'")
        
    # Split the path and extract the relevant parts
    path_parts = output_path.split('/')
    
    cam = int(path_parts[1].split('-')[0].split('cam')[1])
    ccd = int(path_parts[1].split('-')[1].split('ccd')[1])
    numbers = path_parts[2:6]
    gaia_id = path_parts[-1].split('-')[1]

    return [int(gaia_id), cam, ccd, *numbers]

def lc_path(lc_fits_dir, sector_str, args):
    '''
    Construct the path to the light curve file given the parameters

    Parameters
    ----------
    lc_fits_dir: str, path to the directory containing the light curve files
    sector_str: str, sector string
    args: list, list of parameters extracted from the line
    
    Returns
    ------- 
    path: str, path to the light curve file
    '''

    return os.path.join(lc_fits_dir, f'cam{args["cam"]}-ccd{args["ccd"]}/{args["fp1"]}/{args["fp2"]}/{args["fp3"]}/{args["fp4"]}/hlsp_tglc_tess_ffi_gaiaid-{args["gaiadr3_id"]}-{sector_str}-cam{args["cam"]}-ccd{args["ccd"]}_tess_v1_llc.fits')

def parse_curl_commands(sh_file):
    '''
    Parse the curl commands from the .sh file

    Parameters
    ----------
    sh_file: str, path to the .sh file
    
    Returns
    ------- 
    params: list, list of parameters extracted from the lines
    '''

    lines = read_sh(sh_file)
    params = list(parse_line(line) for line in lines)
    return params 

def create_sector_catalog(sector: int, tess_data_path: str, save_catalog: bool = False, tiny: bool = True):
    '''
    Create a sector catalog from the .sh file. Sector catalogs contains: gaiadr3_id, cam, ccd, fp1, fp2, fp3, fp4 
    for each light curve in the sector.

    Parameters
    ----------
    sector: int, sector number
    tess_data_path: str, path to the directory containing the light curve fits files
    tiny: bool, if True, only use a small sample of 100 objects for testing

    Returns
    ------- 
    catalog: astropy Table, sector catalog
    '''

    sector_str = f's{sector:04d}'
    sh_fp = os.path.join(tess_data_path, f'{sector_str}/{sector_str}_fits_download_script.sh')
    params = parse_curl_commands(sh_fp)
    column_names = ['gaiadr3_id', 'cam', 'ccd', 'fp1', 'fp2', 'fp3', 'fp4']
    catalog = Table(rows=params, names=column_names)

    if tiny:
        catalog = catalog[0:100]

    # Merge with target list to get RA-DEC
    csv_fp = os.path.join(tess_data_path, f'{sector_str}/{sector_str}_target_list.csv')
    target_list = Table.read(csv_fp, format='csv')
    target_list.rename_column('#GAIADR3_ID', 'gaiadr3_id')

    catalog = join(catalog, target_list, keys='gaiadr3_id', join_type='inner')

    catalog['healpix'] = hp.ang2pix(_healpix_nside, catalog['RA'], catalog['DEC'], lonlat=True, nest=True)

    if save_catalog:
        output_fp = os.path.join(tess_data_path, f'{sector_str}', f'{sector_str}-catalog{"_tiny" if tiny else ""}.hdf5')
        catalog.write(output_fp, format='hdf5', overwrite=True, path=output_fp)
        print(f"Saved catalog to {output_fp}")
    return catalog

def get_fits_lightcurve(catalog_row, sector, output_dir):
    '''
    Download the light curve file using the curl command and save it to the output file

    Parameters
    ----------
    curl_cmd: str, curl command
    output_fp: str, path to the output file
    
    Returns
    ------- 
    success: bool, True if the download was successful, False otherwise
    '''

    sector_str = f's{sector:04d}'
    path = f'cam{catalog_row["cam"]}-ccd{catalog_row["ccd"]}/{catalog_row["fp1"]}/{catalog_row["fp2"]}/{catalog_row["fp3"]}/{catalog_row["fp4"]}/hlsp_tglc_tess_ffi_gaiaid-{catalog_row["gaiadr3_id"]}-{sector_str}-cam{catalog_row["cam"]}-ccd{catalog_row["ccd"]}_tess_v1_llc.fits'
    url = f'https://archive.stsci.edu/hlsps/tglc/{sector_str}/' + path 

    output_fp = os.path.join(output_dir, path)

    # Create output directory if it doesn't exist
    os.makedirs(os.path.dirname(output_fp), exist_ok=True)

    cmd = f'wget {url} -O {output_fp}'
    try:
        subprocess.run(cmd, shell=True, check=True, text=True, capture_output=True)
        return True, f"Successfully downloaded: {output_fp}"
    
    except subprocess.CalledProcessError as e:
        return False, f"Error downloading using the following cmd: {cmd}: {e.stderr}"

def processing_fn(
        row
):
    ''' 
    Process a single light curve file into the standard format 

    Parameters
    ----------
    row: astropy Row, a single row from the sector catalog containing the object descriptors: gaiadr3_id, cam, ccd, fp1, fp2, fp3, fp4
    
    Returns
    ------- 
    lightcurve: dict, light curve in the standard format
    i.e. 
        {
            'TIC_ID': tess id,
            'gaiadr3_id': gaia id,
            'time': obs_times: arr_like,
            'psf_flux': psf_fluxes: arr_like,
            'psf_flux_err': psf_flux_err: float,
            'aper_flux': aperture_fluxes: arr_like,
            'aper_flux_err': aperture_flux_err: float,
            'tess_flags': tess_flags: arr_like,
            'tglc_flags': tglc_flags: arr_like,
            'RA': ra: float,
            'DEC': dec # More columns maybe required...
        }
    '''
    sector_str = f's{23:04d}' # remove hardcode - need to make this dynamic by including the sector as an argument

    fits_path = f'./{sector_str}/cam{row["cam"]}-ccd{row["ccd"]}/{row["fp1"]}/{row["fp2"]}/{row["fp3"]}/{row["fp4"]}/hlsp_tglc_tess_ffi_gaiaid-{row["gaiadr3_id"]}-s0023-cam{row["cam"]}-ccd{row["cam"]}_tess_v1_llc.fits'

    try:
        with fits.open(fits_path, mode='readonly', memmap=True) as hdu:
            # Filter by TESS quality flags - TO-DO

            return {'TIC_ID': row['TIC_ID'],
                    'gaiadr3_id': row['gaiadr3_id'],
                    'time': hdu[1].data['time'],
                    'psf_flux': hdu[1].data['psf_flux'],
                    'psf_flux_err': hdu[1].header['psf_err'],
                    'aper_flux': hdu[1].data['aperture_flux'],
                    'aper_flux_err': hdu[1].header['aper_err'],
                    'tess_flags': hdu[1].data['TESS_flags'],
                    'tglc_flags': hdu[1].data['TGLC_flags'],
                    'RA': hdu[1].header['ra_obj'],
                    'DEC': hdu[1].header['dec_obj']
                    }
        
    except FileNotFoundError:
        print(f"File not found: {fits_path}")
        return

def save_in_standard_format(args):
    '''
    Save the standardised light curve dict in a hdf5 file 

    Parameters
    ----------
    args: tuple, tuple of arguments: (catalog, output_filename, tess_data_path)

    Returns
    -------
    success: bool, True if the file was saved successfully, False otherwise
    '''

    catalog, output_filename, tess_data_path = args
    
    if not os.path.exists(os.path.dirname(output_filename)):
        os.makedirs(os.path.dirname(output_filename))

    results = []
    # parallelize this
    for row in catalog:
        results.append(processing_fn(row))

    max_length = max([len(d['time']) for d in results])

    for i in range(len(results)):
        for key in results[i].keys():
            if isinstance(results[i][key], np.ndarray):
                results[i][key] = np.pad(results[i][key], (0,max_length - len(results[i][key])), mode='constant')

    lightcurves = Table({k: [d[k] for d in results]
                     for k in results[0].keys()})
    lightcurves.convert_unicode_to_bytestring()

    with h5py.File(output_filename, 'w') as hdf5_file:
        for key in lightcurves.colnames:
            hdf5_file.create_dataset(key, data=lightcurves[key])
    return 1

def download_sh_script(sector: int, output_path: str, show_progress: bool = False):
    '''
    Download the sh script from the TGLC MAST site (https://archive.stsci.edu/hlsp/tglc)

    Parameters
    ----------
    sector: int, TESS sector number
    output_path: str, path to the output directory
    show_progress: bool, if True, show the progress of the download

    Returns
    -------
    success: bool, True if the file was downloaded successfully, False otherwise

    Raises
    ------
    Exception: if there is an error downloading the file
    ''' 

    sector_str = f's{sector:04d}'
    url = f'https://archive.stsci.edu/hlsps/tglc/download_scripts/hlsp_tglc_tess_ffi_{sector_str}_tess_v1_llc.sh'

    try:
        os.makedirs(output_path, exist_ok=True)
        curl_cmd = f'wget {"--progress=bar:force --show-progress" if show_progress else ""} {url} -O {os.path.join(output_path, f"{sector_str}_fits_download_script.sh")}'
        
        if show_progress: # This could be cleaner
            result = subprocess.run(curl_cmd, shell=True, check=True, text=True)
        else:
            result = subprocess.run(curl_cmd, shell=True, check=True, text=True, capture_output=True)

        if result.returncode == 0:
            print(f"Successfully downloaded: {output_path}")
            return True
        else:
            print(f"Error downloading file: {result.stderr}")
            return False
        
    except Exception as e:
        print(f"Error downloading .sh file from {url}: {e}")
        return
    
def download_target_csv_file(sector: int, output_path: str, show_progress: bool = False):

    '''
    Download the target list csv file from the TGLC MAST site (https://archive.stsci.edu/hlsp/tglc)

    Parameters
    ----------
    sector: int, TESS sector number
    output_path: str, path to the output directory
    show_progress: bool, if True, show the progress of the download

    Returns
    -------
    success: bool, True if the file was downloaded successfully, False otherwise

    '''
    sector_str = f's{sector:04d}'
    url = f'https://archive.stsci.edu/hlsps/tglc/target_lists/{sector_str}.csv'

    try:
        os.makedirs(output_path, exist_ok=True)
        fp = os.path.join(output_path, f"{sector_str}_target_list.csv")
                     
        curl_cmd = f'wget {"--progress=bar:force --show-progress" if show_progress else ""} {url} -O {fp}'
        
        if show_progress:
            result = subprocess.run(curl_cmd, shell=True, check=True, text=True)
        else:
            result = subprocess.run(curl_cmd, shell=True, check=True, text=True, capture_output=True)

        if result.returncode == 0:
            print(f"Successfully downloaded: {fp}")
            return True
        else:
            print(f"Error downloading file: {result.stderr}")
            return False
        
    except Exception as e:
        print(f"Error downloading csv file from {url}: {e}")
        return False
    
def download_sector_catalog_lightcurves(
        sector: int, 
        tess_data_path: str,
        n_processes: int = 4,
        tiny: bool = True
    ):
    '''
    Download the light curves for a given sector and save them in the standard format

    Parameters
    ----------
    sector: int, TESS sector number
    tess_data_path: str, path to the output directory
    n_processes: int, number of processes to use for the parallel download
    tiny: bool, if True, only use a small sample of 100 objects for testing

    Returns
    -------
    results: list, list of tuples containing the success status and the message for each light curve download
    '''
    

    sector_str = f's{sector:04d}'
    catalog_fp = os.path.join(tess_data_path, f'{sector_str}', f'{sector_str}-catalog{"_tiny" if tiny else ""}.hdf5')
    
    # check if sector catalog exists - if not, create it
    if os.path.exists(catalog_fp):
        print(f"Loading existing catalog from {catalog_fp}")
        catalog = Table.read(catalog_fp, format='hdf5')

    else:
        print(f"Creating new catalog for sector {sector}")
        catalog = create_sector_catalog(sector, tess_data_path, save_catalog = False, tiny = False)

    # check if fits_lc path exists or make it

    fits_lc_path = os.path.join(tess_data_path, f'{sector_str}/fits_lcs')
    if not os.path.exists(fits_lc_path):
        os.makedirs(fits_lc_path, exist_ok=True)
    
    # Check what the non-parallel version time is...

    with Pool(n_processes) as pool:
        results = list(tqdm(pool.starmap(get_fits_lightcurve, [(row, sector, fits_lc_path) for row in catalog]), total=len(catalog)))

    if sum([result[0] for result in results]) != len(catalog):
        print("There was an error in the parallel processing of the download of the fits files, some files may not have been downloaded.")

    return results

def convert_fits_to_standard_format(sector: int, catalog: Table, tess_data_path: str, n_processes: int = 4):
    '''
    Convert the fits light curves to the standard format and save them in a hdf5 file

    Parameters
    ----------
    sector: int, TESS sector number
    catalog: astropy.Table, sector catalog
    tess_data_path: str, path to the output directory
    n_processes: int, number of processes to use for the parallel processing

    Returns
    -------
    results: list, list of booleans indicating the success of the conversion for each light curve
    '''

    sector_str = f's{sector:04d}'
    output_dir = f'./tess_data/{sector_str}/MultimodalUniverse' # rename?

    catalog = catalog.group_by(['healpix'])

    map_args = []
    for group in catalog.groups: 
        group_filename = os.path.join(output_dir, '{}/healpix={}/001-of-001.hdf5'.format("TGLC", group['healpix'][0]))
        map_args.append((group, group_filename, tess_data_path))

    with Pool(n_processes) as pool:
        results = list(tqdm(pool.imap(save_in_standard_format, map_args), total=len(map_args)))
    
    if sum(results) != len(map_args):
        print("There was an error in the parallel processing of the fits files to standard format, some files may not have been processed correctly")
    return results

def main(output_dir = './tess_data/', tess_data_path = './tess_data', tiny = True, n_processes = 4):
    '''
    Full function to download the light curves for a given sector and save them in the standard format in hdf5 files

    Parameters
    ----------
    output_dir: str, path to the output directory
    tess_data_path: str, path to the tess data directory
    tiny: bool, if True, only use a small sample of 100 objects for testing
    n_processes: int, number of processes to use for the parallel processing

    Returns
    -------
    None
    '''

    # Download the sh file from the TGLC site

    download_sh_script(23, os.path.join(output_dir, 'sh_file'), show_progress = False) 

    # Download the target list csv file from the TGLC site
    download_target_csv_file(23, os.path.join(output_dir, 's0023/'), show_progress = False)

    # Create the sector catalog
    catalog = create_sector_catalog(23, tess_data_path = output_dir, save_catalog = True, tiny = True)  

    # Download the fits light curves using the sector catalog
    download_sector_catalog_lightcurves(23, output_dir, show_progress = True)

    # count the number of files in the fits_lcs directory
    # Count number of fits files

    sector_str = f's{23:04d}'  # Using sector 23 as example
    fits_dir = f'./tess_data/{sector_str}/fits_lcs'
    
    n_files = 0
    for _, _, files in os.walk(fits_dir):
        n_files += len([f for f in files if f.endswith('.fits')])
    assert n_files == len(catalog), f"Expected {len(catalog)} .fits files in {fits_dir}, but found {n_files}"

    # Process fits to standard format
    convert_fits_to_standard_format(23, catalog, output_dir, n_processes = 4)

    # How can I think of testing this robustly?
    # Load to show the examples.
    tess = load_dataset("./tess_data/s0023/MultimodalUniverse/tess.py", trust_remote_code=True)

    dset = tess.with_format('numpy')['train']
    print(dset)
    example = next(iter(dset))
    print(example)

    print(example['lightcurve']['time'].shape)
    print(example['lightcurve']['aper_flux'])
    print(example['lightcurve']['aper_flux_err'])
    # The formatting is wrong and the actual values are incorrect. This needs fixed.

if __name__ == '__main__':
    main(output_dir = './tess_data/', tess_data_path = './tess_data', tiny = False, n_processes = 4)
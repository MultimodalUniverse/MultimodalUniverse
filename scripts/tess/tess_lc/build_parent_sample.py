import os 
from astropy.io import fits
import numpy as np
from astropy.table import Table, join
import h5py
import healpy as hp
from multiprocessing import Pool, freeze_support
from tqdm import tqdm

_healpix_nside = 16

PIPELINES = ['TGLC']

# To-Do:
# - Scripts for downloading the .sh files and .csv from TGLC MAST
# - Cross-matching across sectors with gaia_id 
# - Write function to create .sh files

def read_sh(fp: str):
    with open(fp, 'r') as f:
        lines = f.readlines()[1:]
    return lines

def parse_line(line: str):
    parts = line.split()
    output_path = parts[4].strip("'")
        
    # Split the path and extract the relevant parts
    path_parts = output_path.split('/')
    
    cam = int(path_parts[1].split('-')[0].split('cam')[1])
    ccd = int(path_parts[1].split('-')[1].split('ccd')[1])
    numbers = path_parts[2:6]
    gaia_id = path_parts[-1].split('-')[1]

    return [int(gaia_id), cam, ccd, *numbers]
        
def parse_curl_commands(sh_file):
    #with Pool(n_processes) as p:
    lines = read_sh(sh_file)
    params = list(parse_line(line) for line in lines)
    return params 

def create_sector_catalog(sector: int, tess_data_path: str):
    sector_str = f's{sector:04d}'
    sh_fp = os.path.join(tess_data_path, f'{sector_str}/hlsp_tglc_tess_ffi_{sector_str}_tess_v1_llc.sh')
    params = parse_curl_commands(sh_fp)
    column_names = ['gaiadr3_id', 'cam', 'ccd', 'fp1', 'fp2', 'fp3', 'fp4']
    catalog = Table(rows=params, names=column_names)
    return catalog

def processing_fn(
        filename,
        object_id
):
    ''' 
    input:
        filename: str, path to the light curve file in fits format
        object_id: str, GAIA ID
    '''

    with fits.open(filename, mode='readonly', memmap=True) as hdu:
        
        # Filter by TESS quality flags

        return {'object_id': object_id,
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

def save_in_standard_format(catalog, output_filename):
    '''
    input:
        catalog: astropy Table, catalog with the following columns: object_id, fp
        output_filename: str, path to the output file
    '''

    if not os.path.exists(os.path.dirname(output_filename)):
        os.makedirs(os.path.dirname(output_filename))

    results = []
    for obj_id, fp in catalog[['TIC_ID', 'fp']]:
        results.append(processing_fn(fp, obj_id))

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

def main(output_dir = './test', tess_data_path = './tess_data', tiny = False, n_jobs = 1):
    
    sector = 23
    sector_str = f's{sector:04d}'

    catalog = create_sector_catalog(23, tess_data_path = "./tess_data")

    lcs = Table.read(os.path.join(tess_data_path, "./s0023/s0023.csv")) # Do this for all sectors
    lcs.rename_column('#GAIADR3_ID', 'gaiadr3_id')

    lcs['healpix'] = hp.ang2pix(_healpix_nside, lcs['RA'], lcs['DEC'], lonlat=True, nest=True)
    
    # Join the catalogs using the gaia_id
    catalog = join(catalog, lcs, keys='gaiadr3_id', join_type='inner')
    catalog.write(os.path.join(tess_data_path, f'{sector_str}', f'{sector_str}-catalog.hdf5'), format='hdf5', overwrite=True, path=os.path.join(tess_data_path, f'{sector_str}', f'{sector_str}-catalog.hdf5'))

    print(len(catalog))
    '''for pipeline in PIPELINES:
        cat_pipeline = catalog

        cat_pipeline = cat_pipeline.group_by(['healpix'])
        
        map_args = []
        for group in cat_pipeline.groups:
            print(group)
            group_filename = os.path.join(output_dir, '{}/healpix={}/001-of-001.hdf5'.format(pipeline.strip(), group['healpix'][0]))
            map_args.append((group, group_filename, tess_data_path, tiny))
    
        results = list(tqdm(save_in_standard_format(catalog, group_filename), total=len(map_args)))
        print(results)'''
    return 


main(output_dir = './test', tess_data_path = './tess_data', tiny = False)
#save_in_standard_format(catalog, './test.h5')



import os
import argparse
from astropy.table import Table, join
import astropy.units as u
from unagi import hsc, task
from multiprocessing import Pool
import healpy as hp
import numpy as np
import h5py
from tqdm import tqdm
from astropy.wcs import WCS
from astropy.nddata import Cutout2D
from astropy.io import fits
from filelock import FileLock

HSC_PIXEL_SCALE = 0.168 # Size of a pixel in arcseconds

_filters = ['HSC-G', 'HSC-R', 'HSC-I', 'HSC-Z', 'HSC-Y']
_utf8_filter_type = h5py.string_dtype('utf-8', 5)
_image_size = 160
_pixel_scale = 0.168
_healpix_nside = 16

def _processing_fn(args):
    """ Function that processes all the tract and patches that fall in a given healpix index
    """
    source_catalog, data_dir, group_filename = args

    # Group the objects by tract and patch
    patches = source_catalog.group_by(['tract', 'patch'])

    # Loop over the bricks
    for patch_cat in patches.groups:
        # Create a cutout for each object in the brick
        out_images = []

        tract = patch_cat['tract'][0]
        patch = patch_cat['patch'][0]
        patch = f"{patch // 100},{patch % 10}"

        # Load all the images for this patch
        images = {}
        try:
            for filter in _filters:
                image_filename = os.path.join(data_dir, f'{filter}/{tract}/{patch}/calexp-{filter}-{tract}-{patch}.fits')
                with fits.open(image_filename) as hdul:
                    images[filter] = {'image': hdul[1].copy(), 
                                      'var': hdul[3].copy(),
                                      'mask': hdul[2].copy()}
                    
                    # Converting mask to binary mask based on a set of flags
                    data = images[filter]['mask'].data 
                    maskclean = np.ones_like(data, dtype=bool)
                    set_maskbits = [0, 1, 8] # Bad pixels, saturated pixels, no data
                    for bit in set_maskbits:
                        maskclean &= (data & 2**bit)==0
                    images[filter]['mask'].data = maskclean.astype(data.dtype)
        except Exception as e:
            print(f"Failed to load image for patch {tract}, {patch}: {e}")
            continue

        for obj in patch_cat:
            # Create a cutout for each band
            ra, dec = obj['ra'], obj['dec']

            # Build image
            image = []
            for band in _filters:
                wcs = WCS(images[band]['image'].header)
                x, y = wcs.all_world2pix(ra, dec, 1)
                position = (x, y)
                size = (_image_size, _image_size)
                image.append(Cutout2D(images[band]['image'].data, position, size, wcs=wcs).data)
            image = np.stack(image, axis=0)

            # Build inverse variance
            var = []
            for band in _filters:
                wcs = WCS(images[band]['var'].header)
                x, y = wcs.all_world2pix(ra, dec, 1)
                position = (x, y)
                size = (_image_size, _image_size)
                var.append(Cutout2D(images[band]['var'].data, position, size, wcs=wcs).data)
            var = np.stack(var, axis=0)

            mask = []
            for band in _filters:
                wcs = WCS(images[band]['mask'].header)
                x, y = wcs.all_world2pix(ra, dec, 1)
                position = (x, y)
                size = (_image_size, _image_size)
                mask.append(Cutout2D(images[band]['mask'].data, position, size, wcs=wcs).data)
            mask = np.stack(mask, axis=0)

            # Compute the PSF FWHM in arcsec
            psf_fwhm = []
            for f in _filters:
                b = f.lower().split('-')[-1]
                psf_mxx = obj[f'{b}_sdssshape_psf_shape11'].filled(fill_value=0)
                psf_myy = obj[f'{b}_sdssshape_psf_shape22'].filled(fill_value=0)
                psf_mxy = obj[f'{b}_sdssshape_psf_shape12'].filled(fill_value=0)
                psf_fwhm.append(2.355 * (psf_mxx * psf_myy - psf_mxy**2)**(0.25)) # in arcsec
            psf_fwhm = np.nan_to_num(np.array(psf_fwhm).astype(np.float32))

            out_images.append({
                    'object_id': obj['object_id'],
                    'image_band': np.array([f.lower().encode("utf-8") for f in _filters], dtype=_utf8_filter_type),
                    'image_ivar': np.nan_to_num(1./var),
                    'image_array': image,
                    'image_mask': mask.astype('bool'),
                    'image_psf_fwhm': psf_fwhm,
                    'image_scale': np.array([_pixel_scale for f in _filters]).astype(np.float32),
            })

        # If we didn't find any images, we return 0
        if len(out_images) == 0:
            continue

        # Aggregate all images into an astropy table
        images = Table({k: [d[k] for d in out_images] for k in out_images[0].keys()})

        # Join on object_id with the input catalog
        catalog = join(source_catalog, images, 'object_id', join_type='inner')
            
        # Create the output directory if it does not exist
        out_path = os.path.dirname(group_filename)
        if not os.path.exists(out_path):
            os.makedirs(out_path, exist_ok=True)

        with FileLock(group_filename + ".lock"):
            if os.path.exists(group_filename):
                # Load the existing file and concatenate the data with current data
                with h5py.File(group_filename, 'a') as hdf5_file:
                    for key in catalog.colnames:
                        shape = catalog[key].shape
                        hdf5_file[key].resize(hdf5_file[key].shape[0] + shape[0], axis=0)
                        hdf5_file[key][-shape[0]:] = catalog[key]
            else:           
                # This is the first time we write the file, so we define the datasets
                with h5py.File(group_filename, 'w') as hdf5_file:
                    for key in catalog.colnames:
                        shape = catalog[key].shape
                        if len(shape) == 1:
                            hdf5_file.create_dataset(key, data=catalog[key], compression="gzip", chunks=True, maxshape=(None,))
                        else:
                            hdf5_file.create_dataset(key, data=catalog[key], compression="gzip", chunks=True, maxshape=(None, *shape[1:]))

        del catalog, images, out_images

    return 1

def extract_cutouts(parent_sample, data_dir,  output_dir, num_processes=1, proc_id=None, nsplits=1):
    """ Extract cutouts for all detections in the parent sample   
    """
    # Load catalog
    parent_sample = Table.read(parent_sample)

    # Subselecting only the part of the catalog that we want this process to handle
    if proc_id is not None:
        assert proc_id < nsplits, "proc_id must be less than nsplits"
        parent_sample.sort(['tract', 'patch'])
        parent_sample = parent_sample[proc_id::nsplits]

    # Add healpix index to the catalog
    parent_sample['healpix'] = hp.ang2pix(_healpix_nside, parent_sample['ra'], parent_sample['dec'], lonlat=True, nest=True)

    # Group objects by healpix index
    groups = parent_sample.group_by('healpix')

    # Loop over the groups
    map_args = []
    for group in groups.groups:
        group_filename = os.path.join(output_dir, 'healpix={}/001-of-001.hdf5'.format(group['healpix'][0]))
        map_args.append((group, data_dir, group_filename))

    # Run the parallel processing
    with Pool(num_processes) as pool:
        results = pool.map(_processing_fn, map_args)                       

    if np.sum(results) == len(groups.groups):
        print('Done!')
    else:
        print("Warning, unexpected number of results, some files may not have been exported as expected")


def main(args):

    # Define the filename for the output catalog resulting from the query 
    sample_name = args.query_file.split('/')[-1].split('.sql')[0]
    catalog_filename = os.path.join(args.output_dir, sample_name+'.fits')
    output_path = os.path.join(args.output_dir, sample_name)

    # Check if ran as part of a slurm job, if so, only the procid will be processed
    slurm_procid = int(os.getenv('SLURM_PROCID')) if 'SLURM_PROCID' in os.environ else None

    # Login to the HSC archive
    archive = hsc.Hsc(dr=args.dr, rerun=args.rerun)

    # Check if the result of the query already exists
    if not os.path.exists(catalog_filename):
        print("Running query...")

        # Read the query file from file into a string, and if the --tiny argument
        # was set, append a LIMIT clause to the query
        with open(args.query_file, 'r') as f:
            query = f.read()
            if args.tiny:
                query += ' LIMIT 10'

        # Run the query
        catalog = archive.sql_query(query, out_file=catalog_filename)

        print("query saved to {}".format(catalog_filename))

    if args.catalog_only:
        return
    
    # Base directory for HSC data
    data_dir = os.path.join(args.data_dir, args.rerun)

    # Extract the cutouts
    extract_cutouts(catalog_filename, data_dir, output_path, 
                    num_processes=args.num_processes, proc_id=slurm_procid, nsplits=args.nsplits)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Takes an SQL query file, runs the query, and returns the cutouts for the objects in the query')
    parser.add_argument('query_file', type=str, help='The path to the SQL query file')
    parser.add_argument('output_dir', type=str, help='The path to the output directory')
    parser.add_argument('data_dir', type=str, help='The path to the HSC data directory')
    parser.add_argument('--nsplits', type=int, default=1, help='The number of splits to use for parallel processing')
    parser.add_argument('--catalog_only', action='store_true', help='Only run the query and save the catalog, do not generate cutouts')
    parser.add_argument('--num_processes', type=int, default=16, help='The number of processes to use for parallel processing')
    parser.add_argument('--rerun', type=str, default='pdr3_dud', help='The rerun to use')
    parser.add_argument('--dr', type=str, default='pdr3', help='The data release to use')
    parser.add_argument('--tiny', action='store_true', help='Use a tiny version of the catalog for testing purposes')
    args = parser.parse_args()
    main(args)

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

HSC_PIXEL_SCALE = 0.168 # Size of a pixel in arcseconds

_filters = ['HSC-G', 'HSC-R', 'HSC-I', 'HSC-Z', 'HSC-Y']
_utf8_filter_type = h5py.string_dtype('utf-8', 5)
_image_size = 144
_pixel_scale = 0.168

def _processing_fn(args):
    catalog, cutouts_filename, output_filename = args

    if not os.path.exists(os.path.dirname(output_filename)):
        os.makedirs(os.path.dirname(output_filename))

    # count how many times we run into problems with the images
    n_problems = 0

    # Create an empty list to store images
    images = []

    with h5py.File(cutouts_filename, 'r') as data:
        # Loop over the indices and yield the requested data
        for row in catalog:

            key = str(row['object_id'])
            hdu = data[key]

            # Get the smallest shape among all images
            s_x = min([hdu[f]['HDU0']['DATA'].shape[0] for f in _filters])
            s_y = min([hdu[f]['HDU0']['DATA'].shape[1] for f in _filters])

            # Raise a warning if one of the images has a different shape than 'smallest_shape'
            for f in _filters:
                if hdu[f]['HDU0']['DATA'].shape != (s_x, s_y):
                    print(f"The image for object {key} has a different shape depending on the band. It's the {n_problems+1}th time this happens.")
                    n_problems += 1
                    break

            # Crop the images to the smallest shape
            image = np.stack([
                hdu[f]['HDU0']['DATA'][:s_x, :s_y].astype(np.float32) for f in _filters
            ], axis=0).astype(np.float32)
            
            # Cutout the center of the image to desired size
            s = image.shape
            center_x = s[1] // 2
            start_x = center_x - _image_size // 2
            center_y = s[2] // 2
            start_y = center_y - _image_size // 2
            image = image[:, 
                        start_x:start_x+_image_size, 
                        start_y:start_y+_image_size]
            assert image.shape == (5, _image_size, _image_size), ("There was an error in reshaping the image to desired size", image.shape, s )

            # Compute the PSF FWHM in arcsec
            psf_fwhm = []
            for f in _filters:
                b = f.lower().split('-')[-1]
                psf_mxx = row[f'{b}_sdssshape_psf_shape11']
                psf_myy = row[f'{b}_sdssshape_psf_shape22']
                psf_mxy = row[f'{b}_sdssshape_psf_shape12']
                psf_fwhm.append(2.355 * (psf_mxx * psf_myy - psf_mxy**2)**(0.25)) # in arcsec
            psf_fwhm = np.array(psf_fwhm).astype(np.float32)

            # Initialize the example with image data
            images.append({
                    'object_id': row['object_id'],
                    'image_band': np.array([f.lower().encode("utf-8") for f in _filters], dtype=_utf8_filter_type),
                    'image_array': image,
                    'image_psf_fwhm': psf_fwhm,
                    'image_scale': np.array([_pixel_scale for f in _filters]).astype(np.float32),
                })
    # Aggregate all images into an astropy table
    images = Table({k: [d[k] for d in images] for k in images[0].keys()})

    # Join on object_id with the input catalog
    catalog = join(catalog, images, keys='object_id', join_type='inner')

    # Making sure we didn't lose anyone
    assert len(catalog) == len(images), "There was an error in the join operation"

    # Save all columns to disk in HDF5 format
    with h5py.File(output_filename, 'w') as hdf5_file:
        for key in catalog.colnames:
            hdf5_file.create_dataset(key, data=catalog[key])

    return 1

def save_in_standard_format(catalog_filename, cutouts_filename, output_dir, num_processes=None):
    """ This function takes care of saving the dataset in the standard format used by the rest of the project
    """
    # Load the catalog
    catalog = Table.read(catalog_filename)
    # Retrieve sample name from the catalog filename
    sample_name = catalog_filename.split('/')[-1].split('.fits')[0]

    # TODO: Add standard column names for extinction corrected magnitudes

    # Add healpix index to the catalog
    catalog['healpix'] = hp.ang2pix(64, catalog['ra'], catalog['dec'], lonlat=True, nest=True)
    
    # Group objects by healpix index
    groups = catalog.group_by('healpix')

    # Loop over the groups
    map_args = []
    for group in groups.groups:
        # Create a filename for the group
        group_filename = os.path.join(output_dir, '{}/healpix={}/001-of-001.hdf5'.format(sample_name,group['healpix'][0]))
        map_args.append((group, cutouts_filename, group_filename))

    print('Exporting aggregated dataset in hdf5 format to disk...')

    # Run the parallel processing
    with Pool(args.num_processes) as pool:
        results = list(tqdm(pool.imap(_processing_fn, map_args), total=len(map_args)))

    if np.sum(results) == len(groups.groups):
        print('Done!')
    else:
        print("Warning, unexpected number of results, some files may not have been exported as expected")


def main(args):

    # Create the output directory if it does not exist
    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)

    # Login to the HSC archive
    archive = hsc.Hsc(dr=args.dr, rerun=args.rerun)

    # Define the filename for the output catalog resulting from the query 
    catalog_filename = os.path.join(args.output_dir, args.query_file.split('/')[-1].split('.sql')[0]+'.fits')
                                    
    # Check if the result of the query already exists
    if not os.path.exists(catalog_filename):
        print("Running query...")

        # Run the query
        catalog = archive.sql_query(args.query_file, from_file=True, out_file=catalog_filename)

        print("query saved to {}".format(catalog_filename))
    else:
        # Read the catalog from disk
        catalog = Table.read(catalog_filename)
        print("catalog loaded from {}".format(catalog_filename))

    cutout_size = [HSC_PIXEL_SCALE*(args.cutout_size//2 + 2.25 + 0.01)* u.Unit('arcsec'), # Size of cutouts in arcsecs, 
                   HSC_PIXEL_SCALE*(args.cutout_size//2 + 2.25)* u.Unit('arcsec')]        # with a small margin, and ugly fix to ensure same size for all bands
    
    # Check if the cutouts already exist
    cutouts_filename = os.path.join(args.output_dir, 'cutouts_%s_%s.hdf'%(args.rerun, 'coadd'))
    if os.path.exists(cutouts_filename):
        print("Cutouts already exist, skipping cutout download")
    else:
        print("Downloading cutouts...")
        # Create a temporary directory for the cutouts
        tmp_dir = os.path.join(args.temp_dir, 'hsc_cutouts')
        if not os.path.exists(tmp_dir):
            os.makedirs(tmp_dir)
            
        # Now that the catalog has been created, we can download the cutouts for each object
        cutouts_filename = task.hsc_bulk_cutout(catalog, 
                                            cutout_size=cutout_size, 
                                            filters=_filters, 
                                            archive=archive,  
                                            nproc=args.num_processes, 
                                            tmp_dir=tmp_dir, 
                                            output_dir=args.output_dir)
        
        print("Dataset saved to {}".format(cutouts_filename))

        # Remove the temporary directory
        os.rmdir(tmp_dir)

    # Save the dataset in the standard format
    save_in_standard_format(catalog_filename, cutouts_filename, args.output_dir, num_processes=args.num_processes)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Takes an SQL query file, runs the query, and returns the cutouts for the objects in the query')
    parser.add_argument('query_file', type=str, help='The path to the SQL query file')
    parser.add_argument('output_dir', type=str, help='The path to the output directory')
    parser.add_argument('--temp_dir', type=str, default='/tmp', help='The path to the temporary download directory')
    parser.add_argument('--cutout_size', type=int, default=_image_size, help='The size of the cutouts to download')
    parser.add_argument('--num_processes', type=int, default=4, help='The number of processes to use for parallel processing (maximum 4)')
    parser.add_argument('--rerun', type=str, default='pdr3_wide', help='The rerun to use')
    parser.add_argument('--dr', type=str, default='pdr3', help='The data release to use')
    args = parser.parse_args()
    main(args)

import os
import argparse
from astropy.table import Table, vstack, join
from tqdm import tqdm
import glob
import h5py
import healpy as hp
import numpy as np
from multiprocessing import Pool

CATALOG_COLUMNS = [
    'inds',
    'ra',
    'dec',
    'release',
    'brickid',
    'objid',
    'z_spec',
    'flux',
    'fiberflux', 
    'psfdepth',
    'psfsize',
    'ebv'
]


_image_size = 152
_pixel_scale = 0.262


def _processing_fn(args):
    catalog, input_files, output_filename = args

    if not os.path.exists(os.path.dirname(output_filename)):
        os.makedirs(os.path.dirname(output_filename))

    keys = catalog['inds']

    # Sort the input files by name
    input_files = sorted(input_files)

    # Open all the data files
    files = [h5py.File(file, 'r') for file in input_files]

    images = []
    indss = []
    # Loop over the indices and yield the requested data
    for i, id in enumerate(keys):
        # Get the entry from the corresponding file
        file_idx = id // 1_000_000
        file_ind = id % 1_000_000
        
        images.append(files[file_idx]['images'][file_ind])
        indss.append(files[file_idx]['inds'][file_ind])

    # Stack the images and indices, and create an astropy table
    images = np.stack(images, axis=0)
    indss = np.stack(indss, axis=0)
    image_cat = Table({'inds': indss, 'images': images})

    # Close all the data files
    for file in files:
        file.close()
    
    # Making sure we foynf the right number of images
    assert len(catalog) == len(images), "There was an error retrieving images"
    # Join on inds with the input catalog
    catalog = join(catalog, image_cat, keys='inds', join_type='inner')
    # Making sure we didn't lose anyone
    assert len(catalog) == len(images), "There was an error in the join operation"
    
    # Save all columns to disk in HDF5 format
    with h5py.File(output_filename, 'w') as hdf5_file:
        for key in catalog.colnames:
            hdf5_file.create_dataset(key, data=catalog[key])

    return 1

def save_in_standard_format(catalog_filename, sample_name, data_path, output_dir, num_processes=None):
    """ This function takes care of saving the dataset in the standard format used by the rest of the project
    """
    # Load the catalog
    catalog = Table.read(catalog_filename)
    
    # Group objects by healpix index
    groups = catalog.group_by('healpix')

    # Loop over the groups
    map_args = []
    input_files = glob.glob(data_path+f'{sample_name}/images_npix152*.h5')
    for group in groups.groups:
        # Create a filename for the group
        group_filename = os.path.join(output_dir, '{}/healpix={}/001-of-001.hdf5'.format(sample_name,group['healpix'][0]))
        map_args.append((group, input_files, group_filename))

    print('Exporting aggregated dataset in hdf5 format to disk...')

    # Run the parallel processing
    with Pool(num_processes) as pool:
        results = list(tqdm(pool.imap(_processing_fn, map_args), total=len(map_args)))

    if np.sum(results) == len(groups.groups):
        print('Done!')
    else:
        print("Warning, unexpected number of results, some files may not have been exported as expected")


def main(args):

    # Create the output directory if it doesn't exist
    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)

    samples_to_process = []
    if args.only_north:
        samples_to_process.append('north')
    elif args.only_south:
        samples_to_process.append('south')
    else:
        samples_to_process = ['north', 'south']

    for sample in samples_to_process:
        print('Processing sample:', sample)
        # Check if the catalog already exists, if so, we skip this part and just tell the user that 
        # we are using the existing catalog
        catalog_filename = os.path.join(args.output_dir, f'decals_catalog_{sample}.fits')
        if os.path.exists(catalog_filename):
            print('Catalogs already exist, skipping catalog creation')
        else:
            # Looping over the downloaded image files to retrieve important catalog information
            catalogs = []
            for file in tqdm(glob.glob(args.data_path+ f'{sample}/images_npix152*.h5')):
                with h5py.File(file) as d:
                    catalogs.append(Table(data=[d[k][:] for k in CATALOG_COLUMNS], 
                                        names=CATALOG_COLUMNS))
            catalog = vstack(catalogs, join_type='exact')
            # Making sure the catalog is sorted by inds in ascending order
            catalog.sort('inds')
            # Add healpix index to the catalog
            catalog['healpix'] = hp.ang2pix(64, catalog['ra'], catalog['dec'], lonlat=True, nest=True)
            catalog.write(catalog_filename, overwrite=True)

        # Next step, export the data into the standard format
        save_in_standard_format(catalog_filename, sample, args.data_path, args.output_dir, num_processes=args.num_processes)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Builds a catalog for the DECALS images of the stein et al. sample')
    parser.add_argument('data_path', type=str, help='Path to the local copy of the data')
    parser.add_argument('output_dir', type=str, help='Path to the output directory')
    parser.add_argument('--num_processes', type=int, default=1, help='Number of parallel processes to use')
    parser.add_argument('--only_north', action='store_true', help='Only process the north sample')
    parser.add_argument('--only_south', action='store_true', help='Only process the south sample')
    args = parser.parse_args()

    main(args)
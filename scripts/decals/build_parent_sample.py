import os
import argparse
from astropy.table import Table, vstack
from tqdm import tqdm
import glob
import h5py

CATALOG_COLUMNS = [
    'inds',
    'ra',
    'dec',
    'release',
    'brickid',
    'objid',
    'z_spec',
    'flux'
]

def main(args):
    # Looping over the downloaded image files to retrieve important catalog information
    catalogs = []
    for file in tqdm(glob.glob(args.data_path+'north/images_npix152*.h5')):
        with h5py.File(file) as d:
            catalogs.append(Table(data=[d[k][:] for k in CATALOG_COLUMNS], 
                                  names=CATALOG_COLUMNS))
    catalog = vstack(catalogs, join_type='exact')
    # Making sure the catalog is sorted by inds in ascending order
    catalog.sort('inds')
    # Save the catalog
    catalog_filename = os.path.join(args.output_dir, 'decals_catalog_north.fits')
    catalog.write(catalog_filename, overwrite=True)

    # Now doing the same thing for the south sample
    catalogs = []
    for file in tqdm(glob.glob(args.data_path+'south/images_npix152*.h5')):
        with h5py.File(file) as d:
            catalogs.append(Table(data=[d[k][:] for k in CATALOG_COLUMNS], 
                                  names=CATALOG_COLUMNS))
    catalog = vstack(catalogs, join_type='exact')
    # Making sure the catalog is sorted by inds in ascending order
    catalog.sort('inds')
    # Save the catalog
    catalog_filename = os.path.join(args.output_dir, 'decals_catalog_south.fits')
    catalog.write(catalog_filename, overwrite=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Builds a catalog for the DECALS images of the stein et al. sample')
    parser.add_argument('data_path', type=str, help='Path to the local copy of the data')
    parser.add_argument('output_dir', type=str, help='Path to the output directory')
    args = parser.parse_args()

    main(args)
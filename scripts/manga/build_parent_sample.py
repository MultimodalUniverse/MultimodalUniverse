
import argparse
import pathlib
from multiprocessing import Pool
import numpy as np

from astropy.io import fits
from astropy.table import Table, join
from tqdm import tqdm
import healpy as hp
import h5py


_utf8_filter_type = h5py.string_dtype('utf-8', 5)

def selection_fn(catalog):
    return catalog


def process_single_plateifu(args):

    summary_row, filename, object_id = args

    # set up data object
    data = {}
    data["provenance"] = {"project": "SDSS", "survey": "MaNGA", "release": "DR17"}

    # add meta data
    data['observation_id'] = summary_row['plateifu']
    data['ra'] = summary_row['ifura']
    data['dec'] = summary_row['ifudec']
    data['z'] = summary_row['nsa_z']
    data['spaxel_size'] = 0.5
    data['spaxel_size_unit'] = 'arcsec'

    # Load the CUBE file
    with fits.open(filename) as hdulist:
        flux = hdulist['FLUX'].data
        nwave, ny, nx = flux.shape
        nspaxels = ny * nx

        # compute padding size ; pad up to 96
        # pad_arr is 0 for z-axis, and padding for (before, after) in each spatial axis
        padding = int((96 - nx) / 2)
        pad_arr = ((0, 0), (padding, padding), (padding, padding))

        # repad flux
        flux = np.pad(flux, pad_arr)
        nwave, ny, nx = flux.shape
        nspaxels = ny * nx

        # create x, y array indices
        y, x = np.indices((nx, ny))
        x = x.reshape(1, nspaxels)
        y = y.reshape(1, nspaxels)

        # reshape and grab arrays
        # pad mask array with 1024 to indicate as DONOTUSE
        # pad rest with 0s
        flux = flux.reshape(nwave, nspaxels)
        ivar = np.pad(hdulist['IVAR'].data, pad_arr).reshape(nwave, nspaxels)
        mask = np.pad(hdulist['FLUX'].data, pad_arr, constant_values=1024).reshape(nwave, nspaxels)
        lsf = np.pad(hdulist['LSFPOST'].data, pad_arr).reshape(nwave, nspaxels)

        wave = hdulist['WAVE'].data.astype(np.float32)
        wave = np.repeat(wave[:, np.newaxis], nspaxels, axis=1)

        # add spaxels

        # combine the data together
        keys = ['flux', 'ivar', 'mask', 'lsf_sigma', 'lambda', 'x', 'y']
        zz = zip(flux.T, ivar.T, mask.T, lsf.T, wave.T, x[0, :], y[0, :])
        spaxels = [dict(zip(keys, values)) for values in zz]
        data['spaxels'] = spaxels

        # add images
        images = []
        filters = np.array(['g', 'r', 'i', 'z'], dtype=_utf8_filter_type)
        img = create_images(hdulist, 'img', pad_arr[1:])
        psf = create_images(hdulist, 'psf', pad_arr[1:])
        images.append({
            "image_band": filters,
            "image_array": img,
            "image_psf": psf,
            "image_scale": np.array([0.5] * 4),
            "image_scale_units": b"arcsec"
        })
        data['images'] = images

    # Return the results
    return data


def create_images(hdu, image_type, pad_arr):
    if image_type == 'img':
        g = np.pad(hdu['GIMG'].data, pad_arr).astype(np.float32)
        r = np.pad(hdu['RIMG'].data, pad_arr).astype(np.float32)
        i = np.pad(hdu['IIMG'].data, pad_arr).astype(np.float32)
        z = np.pad(hdu['ZIMG'].data, pad_arr).astype(np.float32)
    elif image_type == 'psf':
        g = np.pad(hdu['GPSF'].data, pad_arr).astype(np.float32)
        r = np.pad(hdu['RPSF'].data, pad_arr).astype(np.float32)
        i = np.pad(hdu['IPSF'].data, pad_arr).astype(np.float32)
        z = np.pad(hdu['ZPSF'].data, pad_arr).astype(np.float32)

    img_arr = np.stack([g, r, i, z])
    return img_arr


def process_healpix_group(args):
    hp_grp, output_filename, data_path = args

    # Create the output directory if it does not exist
    path = pathlib.Path(output_filename)
    path.parent.mkdir(parents=True, exist_ok=True)

    # Preparing the arguments for the parallel processing
    map_args = []
    for row in hp_grp:
        plateifu = row['plateifu']
        # find the data cube filepath
        files = pathlib.Path(data_path).rglob(f'*{plateifu}*LOGCUBE.fits*')
        for file in files:
            if file.exists():
                map_args.append((row, file, plateifu))

    # Process all files
    results = []
    for args in map_args:
        results.append(process_single_plateifu(args))

    # Save all results to disk in HDF5 format
    with h5py.File(output_filename, 'w') as hdf:
        prov = results[0]
        hdf.attrs['project'] = prov['project']
        hdf.attrs['survey'] = prov['survey']
        hdf.attrs['release'] = prov['release']

        for res in results:
            obsid = res['observation_id']
            hdf.create_group(obsid)
            hg = hdf[obsid]

            hg.attrs['observation_id'] = res['observation_id']
            hg.attrs['z'] = res['z']
            hg.attrs['ra'] = res['ra']
            hg.attrs['dec'] = res['dec']
            hg.create_dataset('z', data=res['z'])
            hg.create_dataset('ra', data=res['ra'])
            hg.create_dataset('dec', data=res['dec'])

            spax = Table(res['spaxels'])
            hg.create_dataset('spaxels', data=spax)

            im = Table({k: [d[k] for d in res['images']] for k in res['images'][0].keys()})
            hg.create_dataset('images', data=im)

    return 1


def process_files(manga_data_path, output_dir, num_processes: int = 10):
    # Load the catalog file and apply main cuts
    path = pathlib.Path(manga_data_path) / 'drpall-v3.1.1.fits'
    catalog = Table.read(path, hdu='MANGA')
    catalog = catalog[selection_fn(catalog)]

    # Add healpix index to the catalog, and group the table
    catalog['healpix'] = hp.ang2pix(64, catalog['IFURA'], catalog['IFUDEC'], lonlat=True, nest=True)
    hp_groups = catalog.group_by(['healpix'])

    # Preparing the arguments for the parallel processing
    map_args = []
    for group in hp_groups.groups:
        # Create a filename for the group
        path = pathlib.Path(output_dir) / f'manga/healpix={group["healpix"][0]}/001-of-001.hdf5'
        map_args.append((group, path, manga_data_path))

    # Run the parallel processing
    with Pool(num_processes) as pool:
        results = list(tqdm(pool.imap(process_healpix_group, map_args), total=len(map_args)))

    if sum(results) != len(map_args):
        print("There was an error in the parallel processing, some files may not have been processed correctly")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Extracts IFU data from all SDSS MaNGA downloaded')
    parser.add_argument('manga_data_path', type=str, help='Path to the local copy of the SDSS MaNGA data')
    parser.add_argument('output_dir', type=str, help='Path to the output directory')
    parser.add_argument('--num_processes', type=int, default=10, help='The number of processes to use for parallel processing')
    args = parser.parse_args()

    process_files(args)
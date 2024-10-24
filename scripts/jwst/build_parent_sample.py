import argparse
import glob
import os
import pickle
import tarfile

import h5py
import healpy as hp
import numpy as np
import wget
from astropy.coordinates import SkyCoord
from astropy.io import fits
from astropy.nddata.utils import Cutout2D
from astropy.table import Table, join
from astropy.wcs import WCS
from bs4 import BeautifulSoup

_healpix_nside = 16

def get_pixel_scale(header):
    # Create a WCS object from the header
    wcs_info = WCS(header)

    # Now, calculate the pixel scale.
    # We'll use a typical approach by measuring the scale at the reference pixel.
    # CD_ij elements give the transformation from pixel to world coordinates.
    # The pixel scale is the norm of the CD matrix's column vectors for most WCS systems.

    # Extract the CD matrix elements from the WCS object
    cd = wcs_info.wcs.cd  # This contains CD1_1, CD1_2, CD2_1, CD2_2

    # Calculate the pixel scale for each direction, assuming square pixels and small angles.
    # We'll convert from degrees to arcseconds by multiplying by 3600.
    pixel_scale_x = np.sqrt(cd[0, 0] ** 2 + cd[1, 0] ** 2) * 3600  # X direction

    return pixel_scale_x  # assuming rectangular


def download_jwst_DJA(base_url: str, output_directory: str, field_identifier: str, filter_list: list[str]):
    # make sure the output directory exists
    os.makedirs(output_directory, exist_ok=True)
    os.chdir(output_directory)

    # Download the index.html file
    file = wget.download(base_url)

    # Read the content of the file
    with open(file, "r") as f:
        file_content = f.read()

    # Parse the HTML content
    soup = BeautifulSoup(file_content, "html.parser")

    jwstfiles = []
    for temp in soup.find_all("a"):
        if (field_identifier in temp["href"]) and ("_sci" in temp["href"]):
            jwstfiles.append(temp["href"])

    # Print and download files
    for url in jwstfiles:
        # Extract the filename from the URL
        filename = url.split("/")[-1]

        # Determine the filter from the filename
        temp = filename.split("_")[0].split("-")
        if temp[-1] != "clear":
            filter_name = temp[-1]
        else:
            filter_name = temp[-2]

        # Construct the full local filepath
        full_local_path = os.path.join(output_directory, filename)
        # Download the file to the specified output directory
        if (filter_name in filter_list) and (not os.path.isfile(full_local_path)):
            print(f"... {full_local_path} does not exist - downloading from {url}...")
            wget.download(url)

    photoz_url = None
    phot_url = None

    relevant_hrefs = [a.get('href') for a in soup.find_all('a', href=lambda href: href and field_identifier in href)]
    for href in relevant_hrefs:
        if "photoz" in href:
            photoz_url = href
        if "phot_apcorr.fits" in href:
            phot_url = href
        
        if photoz_url and phot_url:
            break  # Exit the loop if both URLs are found

    if not photoz_url:
        raise ValueError(f"No photoz file found for {field_identifier}")
    if not phot_url:
        raise ValueError(f"No photometry file found for {field_identifier}")

    # download the photoz file
    filename = photoz_url.split("/")[-1]
    full_local_path = os.path.join(output_directory, filename)
    if not os.path.isfile(full_local_path):
        print(f"... Downloading to {full_local_path}...")
        file = wget.download(photoz_url)
        # unzip the file
        tar = tarfile.open(file)
        tar.extractall()
        tar.close()

    # download photometric catalog
    filename = phot_url.split("/")[-1]
    if not os.path.isfile(full_local_path):
        wget.download(phot_url)
    phot_table = Table.read(filename)

    # read it in as a table
    fnames = os.listdir(".")
    for fname in fnames:
        if "eazypy.zout" in fname:
            photz_table = Table.read(fname)

    joint_table = join(
        phot_table,
        photz_table,
    )

    # phot_table
    return joint_table


def _cut_stamps_fn(
    directory_path, phot_table, field_identifier, filter_list, subsample="all", mag_cut=25.5, image_size=96
):
    pattern = field_identifier + "*fits.gz"

    # Construct the full path pattern
    full_path_pattern = pattern

    # List all matching files
    matching_files = glob.glob(full_path_pattern)

    # filter out faint objects - CHECK
    try:
        flux = phot_table['f444w_tot_corr']
    except KeyError:
        print(f"Column 'f444w_tot_corr' not found. Available columns are: {phot_table.colnames}")
    
    mag = 2.5*np.log10(flux**-6/3631)
    phot_table = phot_table[(mag<mag_cut)]

    if subsample == "all":
        # Use all entries in phot_table
        subsample_indices = np.arange(
            len(phot_table)
        )  # This will create an iterable over all indices
    else:
        # Use the first 'subsample' number of entries in phot_table
        subsample_indices = np.random.choice(
            len(phot_table), size=100, replace=False
        )  # Ensures subsample does not exceed actual size

    print("... filters:", filter_list)
    for f in filter_list:
        # Loop over the matching files
        for file_path in matching_files:
            pickle_filename = (
                "jwst_"
                + field_identifier
                + "_"
                + f
                + "_sample_"
                + str(subsample)
                + "_forastropile.pkl"
            )

            if f in file_path and not os.path.isfile(pickle_filename):
                print("... reading filter " + f)
                im = fits.open(file_path)
                sci = im["PRIMARY"].data
                wcs = WCS((im["PRIMARY"].header))
                pixel_scale = get_pixel_scale(im["PRIMARY"].header)
                # Define the filename for the pickle file

                ravec = []
                decvec = []
                JWST_stamps = []
                idvec = []
                for idn, ra, dec in zip(
                    phot_table["object_id"][subsample_indices].value,
                    phot_table["ra"][subsample_indices].value,
                    phot_table["dec"][subsample_indices].value,
                ):
                    try:
                        position = SkyCoord(ra, dec, unit="deg")
                        stamp = Cutout2D(sci, position, image_size, wcs=wcs, mode='partial', fill_value=0)
                        
                        ravec.append(ra)
                        decvec.append(dec)
                        idvec.append(idn)
                        
                        if np.max(stamp.data) <= 0:
                            print(f"Invalid stamp for object {idn:8d}. Appending blank image. {np.count_nonzero(stamp.data == 0)}, {stamp.data.shape}")
                            JWST_stamps.append(np.zeros((image_size, image_size)))
                        else:
                            norm = stamp.data
                            JWST_stamps.append(norm)
                    except Exception as e:
                        print(f"Exception {e} for object {idn:8d}. Appending a blank image.")
                        JWST_stamps.append(np.zeros((image_size, image_size)))
                        idvec.append(idn)

                        ravec.append(ra)
                        decvec.append(dec)

                # Open a file for writing the pickle data
                print(pickle_filename)
                with open(pickle_filename, "wb") as pickle_file:
                    # Create a dictionary to store your lists
                    data_to_store = {
                        "JWST_stamps": JWST_stamps,
                        "idvec": idvec,
                        "ravec": ravec,
                        "decvec": decvec,
                        "phot_table": phot_table[subsample_indices],
                        "pixel_scale": pixel_scale,
                    }
                    # Use pickle.dump() to store the data in the file
                    pickle.dump(data_to_store, pickle_file)

                print(f"... Data stored in {pickle_filename}")
    return 1


def _processing_fn(image_dir: str, output_dir: str, field_identifier: str, subsample: str, filter_list: list[str], image_size: int):
    image_folder = image_dir
    output_folder = output_dir
    
    os.chdir("..")

    # Create an empty list to store images
    images = []

    # Initialize the dictionary
    JWST_multilambda = {}
    for f in filter_list:
        pickle_filename = os.path.join(
            image_folder,
            "jwst_"
            + field_identifier
            + "_"
            + f
            + "_sample_"
            + str(subsample)
            + "_forastropile.pkl",
        )  # Update the path as needed
        with open(pickle_filename, "rb") as pfile:
            data_loaded = pickle.load(pfile)

            # Accessing the lists from the loaded data
            JWST_stamps = data_loaded["JWST_stamps"]

        # assumng these are all the same for all objects
        catalog = data_loaded["phot_table"]
        pixel_scale = data_loaded["pixel_scale"]

        JWST_multilambda[f] = np.array(JWST_stamps)

        # Add healpix index to the catalog
        catalog["index"] = np.arange(len(catalog))

        catalog["healpix"] = hp.ang2pix(
            _healpix_nside, catalog["ra"], catalog["dec"], lonlat=True, nest=True
        )

        # Group objects by healpix index
        groups = catalog.group_by("healpix")

    # Loop over the groups
    for group in groups.groups:
        healpix = group['healpix'][0]
        group_dir = os.path.join(
            output_folder,
            f"{field_identifier}_{subsample}_{image_size}/healpix={healpix}"
        )
        
        # Create the directory if it doesn't exist
        os.makedirs(group_dir, exist_ok=True)

        # Split the group into chunks of 1024 objects
        chunk_size = 1024
        for chunk_index, chunk_start in enumerate(range(0, len(group), chunk_size)):
            chunk = group[chunk_start:chunk_start + chunk_size]
            
            # Create a filename for the chunk
            chunk_filename = os.path.join(group_dir, f"{chunk_index+1:04d}-of-{(len(group)-1)//chunk_size+1:04d}.hdf5")

            if not os.path.exists(chunk_filename):
                images = []
                for row in chunk:
                    c = row['index']
                    id = row['object_id']

                    # Get the smallest shape among all images
                    smallest_shape = min(JWST_multilambda[f].shape[1:] for f in filter_list)
                    s_x, s_y = smallest_shape

                    # Crop the images to the smallest shape
                    image = np.stack(
                        [
                            JWST_multilambda[f][c][:s_x, :s_y].astype(np.float32)
                            for f in filter_list
                        ],
                        axis=0,
                    ).astype(np.float32)

                    # Cutout the center of the image to desired size
                    s = image.shape
                    center_x = s[1] // 2
                    start_x = center_x - image_size // 2
                    center_y = s[2] // 2
                    start_y = center_y - image_size // 2
                    image = image[
                        :, start_x : start_x + image_size, start_y : start_y + image_size
                    ]
                    assert image.shape == (len(filter_list), image_size, image_size), (
                        "There was an error in reshaping the image to desired size."
                        "Probably a fiter is missing?"
                        "Check the available list of filters for the survey",
                        image.shape,
                        s,
                    )

                    # Automatically create _filters by formatting each entry in _filter_list (astropile nomenclature)
                    filters = [f"jwst_nircam_{filter_name}" for filter_name in filter_list] # Are all of these NIRCam? No MIRI filters?
                    images.append({
                        "object_id": id,
                        "image_band": np.array([f.lower().encode("utf-8") for f in filters], dtype=_utf8_filter_type),
                        "image_array": image,
                        # Image psf using FWHM in pixels and pixel scale in arcsec as calculated from the header
                        "image_psf_fwhm": np.array([0.015 for filter_name in filter_list]).astype(np.float32),
                        "image_scale": np.array([pixel_scale for f in filters]).astype(np.float32),
                    })

                # Aggregate all images into an astropy table
                images = Table({k: [d[k] for d in images] for k in images[0].keys()})

                # Join on object_id with the input catalog
                chunk_catalog = join(chunk, images, keys="object_id", join_type="inner")

                # Making sure we didn't lose anyone
                assert len(chunk_catalog) == len(images), f"There was an error in the join operation len(chunk_catalog) != len(images): {len(chunk_catalog)} != {len(images)}"

                # Save all columns to disk in HDF5 format
                with h5py.File(chunk_filename, "w") as hdf5_file:
                    for key in chunk_catalog.colnames:
                        hdf5_file.create_dataset(key, data=chunk_catalog[key])

                print(f"... Saved HDF5 chunk: {chunk_filename}")

    print("... Finished processing all groups")
    return 1


# Initial survey information
from info import _NIRCAM_BANDS, _MIRI_BANDS, _BAND_EXTS, _FLOAT_FEATURES, _SURVEYS_INFO

_utf8_filter_type = h5py.string_dtype("utf-8", 17)


def main(output_dir: str, image_dir: str, survey: str, subsample: str, mag_cut: float, image_size: int):
    # Create the output directory if it does not exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    version = _SURVEYS_INFO[survey]['version']
    base_url = _SURVEYS_INFO[survey]['base_url']
    filter_list = _SURVEYS_INFO[survey]['filters']
    print(f"base_url in main: {base_url}")

    if 'tiny' in subsample:
        print("... Creating a tiny dataset ...")
        filter_list = filter_list[0:2]

    field_identifier = survey + "-grizli-" + version  # version of the images
    image_dir = os.path.join(image_dir, field_identifier)
    print("... images will be saved in directory: ", image_dir)
    print("... dataset will be stored in directory: ", output_dir)

    print(f"... Downloading data for {field_identifier}...")
    phot_table = download_jwst_DJA(base_url, image_dir, field_identifier, filter_list)
    phot_table.rename_column("id", "object_id")
    print(f"... Generating cutouts for {field_identifier}...")
    _cut_stamps_fn(
        image_dir, phot_table, field_identifier, filter_list, 
        subsample=subsample, mag_cut=mag_cut, image_size=image_size
    )
    print(f"... Saving to hdf5 for {field_identifier}...")
    _processing_fn(image_dir, output_dir, survey+'-grizli-'+version, subsample, filter_list, image_size)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Downloads JWST data from DJA from specified survey"
    )

    parser.add_argument(
        "--output_dir", type=str, help="The path to the output directory", default="."
    )
    parser.add_argument(
        "--image_dir",
        type=str,
        help="The path to the temporary download directory",
        default=".",
    )
    parser.add_argument(
        "survey",
        type=str,
        help="Survey name. Currently supported survey keywords are: ceers-full,ngdeep,primer-uds,gds,gdn",
    )
    parser.add_argument(
        "--subsample",
        type=str,
        default="all",
        help="all or tiny. tiny downloads a random subset of 100 objects for testing purposes.",
    )
    parser.add_argument(
        "--mag_cut",
        type=float,
        default=25.5,
        help="Magnitude cut for the sample.",
    )
    parser.add_argument(
        "--image_size",
        type=int,
        default=96,
        help="Image size.",
    )

    args = parser.parse_args()
    main(args.output_dir, args.image_dir, args.survey, args.subsample, args.mag_cut, args.image_size)


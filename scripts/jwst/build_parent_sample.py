import os
import argparse
from astropy.table import Table, join
import astropy.units as u
from astropy.io import fits
from astropy.wcs import WCS
from astropy.nddata.utils import Cutout2D
from astropy.coordinates import SkyCoord
from multiprocessing import Pool
import numpy as np
import h5py
from tqdm import tqdm
import json
import wget
from bs4 import BeautifulSoup
import tarfile
import healpy as hp
import glob
import pickle

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
    pixel_scale_x = np.sqrt(cd[0,0]**2 + cd[1,0]**2) * 3600  # X direction
    pixel_scale_y = np.sqrt(cd[0,1]**2 + cd[1,1]**2) * 3600  # Y direction
    
    return pixel_scale_x  # assuming rectangular



def download_jwst_DJA(base_url,output_directory,field_identifier,filter_list):

    # make sure the output directory exists
    try:
        os.chdir(output_directory)
    except:
        print('output directory not found, made the dir.')
        os.mkdir(output_directory)
        os.chdir(output_directory)

    
    # Download the index.html file
    file = wget.download(base_url)

    # Read the content of the file
    with open(file, "r") as f:
        file_content = f.read()

    # Parse the HTML content
    soup = BeautifulSoup(file_content, 'html.parser')

    jwstfiles = []
    for temp in soup.find_all('a'):
        if (field_identifier in temp['href']) and ('_sci' in temp['href']):
            
            jwstfiles.append(temp['href'])

    # Print and download files
    for url in jwstfiles:
        # Extract the filename from the URL
        filename = url.split('/')[-1]
        
        # Determine the filter from the filename
        temp = filename.split('_')[0].split('-')
        if temp[-1] != 'clear':
            filter_name = temp[-1]
        else:
            filter_name = temp[-2]

       
        # Construct the full local filepath
        full_local_path = os.path.join(output_directory, filename)
        print(full_local_path)
        # Download the file to the specified output directory
        if (filter_name in filter_list) and (not os.path.isfile(full_local_path)):
            wget.download(url)

    # for the photometry table
    for temp in soup.find_all('a'):
        if (field_identifier in temp['href']) & ('photoz' in temp['href']):
            photoz_url = temp['href']
        if (field_identifier in temp['href']) & ('phot_apcorr.fits' in temp['href']):
            phot_url = temp['href']

    # download the photoz file
    filename = photoz_url.split('/')[-1]
    full_local_path = os.path.join(output_directory, filename)
    if not os.path.isfile(full_local_path):
        print('downloading to', full_local_path )
        file = wget.download(photoz_url)
         # unzip the file
        tar = tarfile.open(file)
        tar.extractall()
        tar.close()

    # download photometric catalog
    filename = phot_url.split('/')[-1]
    if not os.path.isfile(full_local_path):
        wget.download(phot_url)
    phot_table = Table.read(filename)

   

    # read it in as a table
    fnames = os.listdir('.')
    for fname in fnames:
        if 'eazypy.zout' in fname:
            photz_table = Table.read(fname)

    joint_table = join(phot_table, photz_table, )
    
#phot_table
    return joint_table


def _cut_stamps_fn(directory_path,phot_table,field_identifier,filter_list,subsample='all'):
    
    pattern = field_identifier+'*fits.gz'

    # Construct the full path pattern
    full_path_pattern = pattern

    # List all matching files
    matching_files = glob.glob(full_path_pattern)

    if subsample == 'all':
        # Use all entries in phot_table
        subsample_indices = np.arange(len(phot_table))  # This will create an iterable over all indices
    else:
        # Use the first 'subsample' number of entries in phot_table
        subsample_indices = np.random.choice(len(phot_table), size=100, replace=False) # Ensures subsample does not exceed actual size

    print("filters:",filter_list)
    for f in filter_list:
        # Loop over the matching files
        for file_path in matching_files:
            pickle_filename = 'jwst_'+field_identifier+'_'+f+'_sample_'+str(subsample)+'_forastropile.pkl'
            
            if f in file_path and not os.path.isfile(pickle_filename):
                print('reading filter '+f)
                im = fits.open(file_path)
                sci=im['PRIMARY'].data
                wcs = WCS((im['PRIMARY'].header))
                pixel_scale = get_pixel_scale(im['PRIMARY'].header)
                # Define the filename for the pickle file
                



                ravec=[]
                decvec=[]
                JWST_stamps=[]
                idvec=[]
                for idn, ra,dec in zip(phot_table['object_id'][subsample_indices].value, phot_table['ra'][subsample_indices].value, phot_table['dec'][subsample_indices].value):
                    try:
                       
                        position = SkyCoord(ra,dec,unit="deg")
                        stamp = Cutout2D(sci,position,_image_size,wcs=wcs)
                        if np.max(stamp.data)<=0 or np.count_nonzero(stamp.data==0)>10 or stamp.data.shape[0]!=_image_size or stamp.data.shape[1]!=_image_size:
                            JWST_stamps.append(np.zeros((_image_size,_image_size)))
                            idvec.append(idn)
                        
                            ravec.append(ra)
                            decvec.append(dec) 
                            continue
                        
                        norm = stamp.data
                        
                        JWST_stamps.append(norm)
                        idvec.append(idn)
                        
                        ravec.append(ra)
                        decvec.append(dec)  
                        
                    
                        
                    except:
                        print('error..appending a blank image')
                        JWST_stamps.append(np.zeros((_image_size,_image_size)))
                        idvec.append(idn)
                        
                        ravec.append(ra)
                        decvec.append(dec) 
            

                

                # Open a file for writing the pickle data
                print(pickle_filename)
                with open(pickle_filename, 'wb') as pickle_file:
                    # Create a dictionary to store your lists
                    data_to_store = {
                        'JWST_stamps': JWST_stamps,
                        'idvec': idvec,
                        'ravec': ravec,
                        'decvec': decvec,
                        'phot_table': phot_table[subsample_indices],
                        'pixel_scale': pixel_scale
                    }
                    # Use pickle.dump() to store the data in the file
                    pickle.dump(data_to_store, pickle_file)

                print(f'Data stored in {pickle_filename}')
    return 1




def _processing_fn(args):
    image_folder,output_folder, field_identifier, subsample, filter_list = args

    os.chdir('..')
    filter_string = '-'.join(filter_list)
    # count how many times we run into problems with the images
    n_problems = 0

    # Create an empty list to store images
    images = []

    
    
    # Initialize the dictionary
    JWST_multilambda = {}
    for f in filter_list:
        pickle_filename = os.path.join(image_folder,'jwst_'+field_identifier+'_'+f+'_sample_'+str(subsample)+'_forastropile.pkl')  # Update the path as needed
        with open(pickle_filename, 'rb') as pfile:
            data_loaded = pickle.load(pfile)

            # Accessing the lists from the loaded data
            JWST_stamps = data_loaded['JWST_stamps']
        
        # assumng these are all the same for all objects
        idvec = data_loaded['idvec']
        ravec = data_loaded['ravec']
        decvec = data_loaded['decvec']
        catalog = data_loaded['phot_table']
        pixel_scale = data_loaded['pixel_scale']

        JWST_multilambda[f] = np.array(JWST_stamps)


         # Add healpix index to the catalog
        catalog['index'] = np.arange(len(catalog))

        catalog['healpix'] = hp.ang2pix(64, catalog['ra'], catalog['dec'], lonlat=True, nest=True)
    
        # Group objects by healpix index
        groups = catalog.group_by('healpix')

    # Loop over the groups
    for group in groups.groups:
        # Create a filename for the group
        group_filename = os.path.join(output_folder, '{}/healpix={}/001-of-001.hdf5'.format(field_identifier+'_'+str(subsample)+'_'+str(_image_size),group['healpix'][0]))
        # Extract the directory path from the group_filename
        directory_path = os.path.dirname(group_filename)
        

        # Check if the directory exists
        if not os.path.exists(directory_path):
            # If the directory does not exist, create it
            os.makedirs(directory_path)

        
       


    # Loop over the indices and yield the requested data
    if not os.path.exists(group_filename):
        for row in group:
            c = row['index']  # Assuming there is an 'index' column specifying the object's index
            id = row['object_id']  # Assuming there is an 'id' column

            key = str(id)

            

            # Get the smallest shape among all images
            stamp = JWST_multilambda[f][c]
            s_x=stamp.shape[0]
            s_y=stamp.shape[1]

                # Crop the images to the smallest shape
            image = np.stack([
                    JWST_multilambda[f][c][:s_x, :s_y].astype(np.float32) for f in filter_list
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
            assert image.shape == (len(filter_list),_image_size, _image_size), ("There was an error in reshaping the image to desired size. Probably a fiter is missing? Check the available list of filters for the survey", image.shape, s )

             # Automatically create _filters by formatting each entry in _filter_list (astropile nomenclature)
            filters = [f'jwst_nircam_{filter_name}' for filter_name in filter_list]
            images.append({
                        'object_id': id,
                        'image_band': np.array([f.lower().encode("utf-8") for f in filters], dtype=_utf8_filter_type),
                        'image_array': image,
                        'image_psf_fwhm': image,
                        'image_scale': np.array([pixel_scale for f in filters]).astype(np.float32),
                    })
        # Aggregate all images into an astropy table
        images = Table({k: [d[k] for d in images] for k in images[0].keys()})

        # Join on object_id with the input catalog
        catalog = join(catalog, images, keys='object_id', join_type='inner')

        # Making sure we didn't lose anyone
        assert len(catalog) == len(images), "There was an error in the join operation"

        # Save all columns to disk in HDF5 format
        with h5py.File(group_filename, 'w') as hdf5_file:
            for key in catalog.colnames:
                hdf5_file.create_dataset(key, data=catalog[key])

    print('saved hdf5', directory_path)

    return 1



# Initial survey information
surveys_info = {
    'primer-cosmos-east': {
        'filters': ['f115w', 'f150w', 'f200w', 'f277w', 'f356w', 'f444w'],
        'base_url': 'https://s3.amazonaws.com/grizli-v2/JwstMosaics/v7/index.html',
        'version' : 'v7.0',
    },
    'ceers-full': {
    'filters': ['f115w', 'f150w', 'f200w', 'f277w', 'f356w', 'f444w'],
    'base_url': 'https://s3.amazonaws.com/grizli-v2/JwstMosaics/v7/index.html',
     'version' : 'v7.0',
    },
     'ngdeep': {
    'filters': ['f090w', 'f115w', 'f150w', 'f200w', 'f277w', 'f356w', 'f444w'],
    'base_url': 'https://s3.amazonaws.com/grizli-v2/JwstMosaics/v7/index.html',
     'version' : 'v7.2',
    },
    'primer-uds': {
    'filters': ['f090w', 'f115w', 'f150w', 'f200w', 'f277w', 'f356w', 'f444w'],
    'base_url': 'https://s3.amazonaws.com/grizli-v2/JwstMosaics/v6/index.html',
     'version' : 'v6.0',
    },
     'gds': {
    'filters': ['f090w', 'f115w', 'f150w', 'f200w', 'f277w', 'f356w', 'f444w'],
    'base_url': 'https://s3.amazonaws.com/grizli-v2/JwstMosaics/v7/index.html',
     'version' : 'v7.0',
    },
    'gdn': {
    'filters': ['f090w', 'f115w', 'f150w', 'f200w', 'f277w', 'f356w', 'f444w'],
    'base_url': 'https://s3.amazonaws.com/grizli-v2/JwstMosaics/v7/index.html',
     'version' : 'v7.3',
    },
}


_image_size = 96 # can be an argument ? 
_utf8_filter_type = h5py.string_dtype('utf-8', 17)

def main(args):

    # Create the output directory if it does not exist
    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)
    version = surveys_info[args.survey]['version']
    base_url = surveys_info[args.survey]['base_url']
    filter_list = surveys_info[args.survey]['filters']

    if 'tiny' in args.subsample:
        print('I will create a tiny dataset...')
        filter_list_short = []
        filter_list_short.append(filter_list[0])
        filter_list = filter_list_short

    output_dir = args.output_dir


    
   

    field_identifier=args.survey+'-grizli-'+version #version of the images
    image_dir = os.path.join(args.image_dir,field_identifier)
    print('images will be saved in directory: ', image_dir)
    print('dataset will be stored in directory: ', output_dir)

    print(field_identifier)
    print('downloading data')
    phot_table=download_jwst_DJA(base_url,image_dir,field_identifier,filter_list)
    phot_table.rename_column('id','object_id')
    print('cutting stamps')
    _cut_stamps_fn(image_dir,phot_table,field_identifier,filter_list,subsample=args.subsample)
    print('saving to hdf5')
    _processing_fn([image_dir,output_dir,field_identifier,args.subsample,filter_list])

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Downloads JWST data from DJA from specified survey')
    
    parser.add_argument('--output_dir', type=str, help='The path to the output directory',default='.')
    parser.add_argument('--image_dir', type=str, help='The path to the temporary download directory',default='.')
    parser.add_argument('survey', type=str, help='Survey name. Currently supported survey keywords are: ceers-full,ngdeep,primer-uds,gds,gdn')
    parser.add_argument('--subsample', type=str, default='all', help='all or tiny. tiny downloads a random subset of 100 objects for testing purposes.')


    args = parser.parse_args()
    main(args)









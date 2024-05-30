# Astropile - Rafael Martinez-Galarza & Mike Tibbetts (CfA)
# This code dowloads data files from the Chandra Source Catalog
# using the CLI protocol:
# https://cxc.cfa.harvard.edu/ciao/threads/csccli/
# For each X-ray detection (each one associated to a source name 
# and RA and DEC, it brings down the folowing files: 
# 1) Event file - this is the file containing all photon recordings
# 2) Light curve - binned light curves from Gregory-Loredo alg.
# 3) Spectral files - PHA (signal), and ARF and RMF (response)
# 4) Images - fits files containing the counts binned spatially
# 5) PSF - the computed PSF at the position of the detection
# The data downloaded correspond to X-ray sources with enough
# counts to allow for spectral fitting (and a minimum of 50 counts),
# that have S/N of at least 5, and that are at most 5 arcmin off-axis
# in the field of view. Also, all events in the broadband (0.5keV-7.0keV)
# are included


# First, let's get the catalog data, directly from the release
# To donwload the catalog table that associates X-ray sources 
# (and sky coordinates) to each detection, and therefore to
# the data products, use the following:

# Imports
import pyvo as vo
import requests
import argparse
import h5py
import numpy as np

# CSC 2.1 TAP service
tap = vo.dal.TAPService('http://cda.cfa.harvard.edu/csc21tap') # For CSC 2.1


def get_source_detections_ids(args):

    # Define the minimum source counts, minimum significance, and output file
    # Recommend: min_cnts = 4000, min_sig = 40, max_theta = 1
    # This functon will create a list of  IDs
    min_cnts, min_sig, max_theta, output_file,file_path = args


    qry = f"""
    SELECT m.name, m.ra, m.dec, o.obsid, o.obi, o.region_id, o.src_cnts_aper_b,
    o.flux_significance_b, o.flux_aper_b, o.theta, o.flux_bb_aper_b,
    o.gti_mjd_obs, o.hard_hm,o.hard_hs, o.hard_ms, o.var_prob_b, 
    o.var_index_b 
    FROM csc21.master_source m, csc21.master_stack_assoc a, csc21.observation_source o, 
    csc21.stack_observation_assoc b, csc21.stack_source s 
    WHERE ((a.match_type = 'u') AND (o.flux_bb_aper_b IS NOT NULL) 
    AND (o.src_cnts_aper_b > {min_cnts}) AND (o.flux_significance_b > {min_sig}) 
    AND (o.theta < {max_theta})) AND (m.name = a.name) 
    AND (s.detect_stack_id = a.detect_stack_id and s.region_id = a.region_id) 
    AND (s.detect_stack_id = b.detect_stack_id and s.region_id = b.region_id) 
    AND (o.obsid = b.obsid and o.obi = b.obi and o.region_id = b.region_id)
    ORDER BY name ASC
    """

    cat = tap.search(qry)

    # Convert the catalog to an astropy Table format.
    cat = cat.to_table()

    # Save the catalog to HDF5 format
    with h5py.File(file_path+output_file+'_hdf5.hdf5', 'w') as hdf5_file:
        for key in cat.colnames:
            # Check if the column data type is a string
            if cat[key].dtype.kind in ['U', 'S']:
                # Encode Unicode string to byte string
                encoded_strings = np.char.encode(cat[key], 'utf-8')
                hdf5_file.create_dataset(key, data=encoded_strings)
            else:
                # Directly save the column as a dataset for non-string types
                hdf5_file.create_dataset(key, data=cat[key])    

    with open(file_path+output_file+'_ids.txt', 'w') as f:
        for i,element in enumerate(cat['obsid'].data):
            print(str(cat['obsid'].data[i])+'.'+str(cat['obi'].data[i])
                  +'.'+str(cat['region_id'].data[i]), file = f)

    return 1


def retrieve(url, packageset, idx, file_path):
    # This function retreives the data and saves them in tarballs
    response = requests.get(url, params={
        'version': 'cur',  # Current version of the CSC
        'packageset': packageset
    })
    with open(file_path+f'package.{idx}.tar', 'wb') as output:
        output.write(response.content)
    return 1


def main(args):

    # Generate file of ids
    get_source_detections_ids((args.min_cnts,args.min_sig,args.max_theta,args.output_file,args.file_path))

    # This is the url for retrieval to data at the CfA
    url = 'http://cda.cfa.harvard.edu/csccli/retrieve'

    # We will download the data in packages of 50 detections each
    number_of_identifiers_per_request = 50

    packageset = ''
    number_of_identifiers = 0

    # The file below contains the list of detection IDs
    separator = ''
    with open(args.file_path+args.output_file+'_ids.txt', 'r') as input:
        # read header line and ignore it
        input.readline()
    
        while True:
            line = input.readline()
            if '' == line:
                break
            line = line.rstrip()

            number_of_identifiers += 1
        
            # Here we specify which datatypes to download
            #packageset += separator + line + '/lightcurve/b'
            #separator = ','
            packageset += separator + line + '/spectrum/b'
            separator = ','
            packageset += separator + line + '/rmf/b'
            separator = ','
            packageset += separator + line + '/arf/b'
            separator = ','
        
            if 0 == number_of_identifiers % number_of_identifiers_per_request:
                retrieve(url, packageset, int(number_of_identifiers / number_of_identifiers_per_request),args.file_path)

                packageset = ''
                separator = ''
            
                # Print progress with
                # the current set thresholds of S/N, etc.
                print(number_of_identifiers)
        

                if 0 != number_of_identifiers % number_of_identifiers_per_request:
                    retrieve(url, packageset, int(number_of_identifiers / number_of_identifiers_per_request)+1,args.file_pat)

        return 1


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Downloads spectra data from the Chandra Source Catalog')
    parser.add_argument('--min_cnts', type=int, default=40, help='Minumum number of source counts')
    parser.add_argument('--min_sig', type=float, default=5, help='Minimum signal to noise')
    parser.add_argument('--max_theta', type=float, default=10, help='Maximum off-axis angle')
    parser.add_argument('--output_file', type=str, default='file_ids.txt', help='Name of file')
    parser.add_argument('--file_path', type=str, default='./output_data/', help='Path to files. Must be default to work with the Chandra HF dataset class.')
    args = parser.parse_args()

    main(args)


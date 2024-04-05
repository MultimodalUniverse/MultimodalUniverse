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
# The data downloaded (IDs compiled in auxiliary file obsid_obi_regid.txt)
# correspond to X-ray sources with enough counts to allow for spectral
# fitting (and a minimum of 50 counts), that have S/N of at least 5, 
# and that are at most 5 arcmin off-axis in the field of view.
# Also, all events in the broadband (0.5keV-7.0keV) are included


# First, let's get the catalog data, directly from the release
# To donwload the catalog table that associates X-ray sources 
# (and sky coordinates) to each detection, and therefore to
# the data products, use the following:

# Imports
import pyvo as vo
import requests
import argparse

# CSC 2.1 TAP service
tap = vo.dal.TAPService('http://cda.cfa.harvard.edu/csc21tap') # For CSC 2.1


def get_source_detections_ids(args):

    # Define the minimum source counts, minimum significance, and output file
    # Recommend: min_cnts = 50, min_sig = 5, max_theta = 5
    # This functon will create a list of  IDs
    min_cnts, min_sig, max_theta, output_file,file_path = args


    qry = """
    SELECT m.name, m.ra, m.dec, o.obsid, o.obi, o.region_id, o.src_cnts_aper_b,
    o.flux_significance_b, o.flux_aper_b, o.theta, o.flux_bb_aper_b,
    o.gti_mjd_obs, o.hard_hm,o.hard_hs, o.hard_ms, o.var_prob_b, 
    o.var_index_b 
    FROM csc21.master_source m, csc21.master_stack_assoc a, csc21.observation_source o, 
    csc21.stack_observation_assoc b, csc21.stack_source s 
    WHERE ((a.match_type = 'u') AND (o.flux_bb_aper_b IS NOT NULL) 
    AND (o.src_cnts_aper_b > """+str(min_cnts)+""") AND (o.flux_significance_b > """+str(min_sig)+""") 
    AND (o.theta < """+str(max_theta)+""")) AND (m.name = a.name) 
    AND (s.detect_stack_id = a.detect_stack_id and s.region_id = a.region_id) 
    AND (s.detect_stack_id = b.detect_stack_id and s.region_id = b.region_id) 
    AND (o.obsid = b.obsid and o.obi = b.obi and o.region_id = b.region_id)
    ORDER BY name ASC
    """

    cat = tap.search(qry)

    with open(file_path+output_file, 'w') as f:
        for i,element in enumerate(cat['obsid'].data):
            print(str(cat['obsid'].data[i])+'.'+str(cat['obi'].data[i])
                  +'.'+str(cat['region_id'].data[i]), file = f)

    return 1


def retrieve(url, packageset, idx):
    # This function retreives the data and saves them in tarballs
    response = requests.get(url, params={
        'version': 'cur',  # Current version of the CSC
        'packageset': packageset
    })
    with open(f'package.{idx}.tar', 'wb') as output:
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
    with open(args.file_path+args.output_file, 'r') as input:
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
            #packageset += separator + line + '/regimg/b'
            #separator = ','
            #packageset += separator + line + '/regevt3/b'
            #separator = ','
            #packageset += separator + line + '/psf/b'
        
            if 0 == number_of_identifiers % number_of_identifiers_per_request:
                retrieve(url, packageset, int(number_of_identifiers / number_of_identifiers_per_request))

                packageset = ''
                separator = ''
            
                # Print progress with
                # the current set thresholds of S/N, etc.
                print(number_of_identifiers)
        

                if 0 != number_of_identifiers % number_of_identifiers_per_request:
                    retrieve(url, packageset, int(number_of_identifiers / number_of_identifiers_per_request)+1)

        return 1


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Downloads spectra data from the Chandra Source Catalog')
    parser.add_argument('min_cnts', type=int, default=1000, help='Minumum number of source counts')
    parser.add_argument('min_sig', type=float, default=20, help='Minimum signal to noise')
    parser.add_argument('max_theta', type=float, default=1, help='Maximum off-axis angle')
    parser.add_argument('output_file', type=str, default='file_ids.txt', help='Name of file')
    parser.add_argument('file_path', type=str, default='/Users/juan/science/astropile/output_data/', help='Path to files')
    args = parser.parse_args()

    main(args)


# Astropile - Rafael Martinez-Galarza
# This code pre-process Chandra X-ray spectra and creates a standard for it
# It requieres Sherpa - X-ray data analysis package that can be installed from:
# https://sherpa.readthedocs.io/en/latest/install.html

import os
import numpy as np
import pyvo as vo
from astropy.table import Table, join
import glob
from sherpa.astro import ui # CIAO/Sherpa imports
import h5py


def processing_fn(args):
    
    # The argument is a path to where the spectral files live
    catalog, PATH = args
    
    targetids = []    # Target ID
    obs_id = []       # Observation ID
    ener_bin_lo = []  # Low end of the energy bin
    ener_bin_hi = []  # High end of the energy bin
    ener_bin_mid = [] # Mid point of the energy bin
    fluxes = []       # Counts/sec/keV
    errors = []       # Error in count value
    
    # We now use Sherpa to extract the spectrum
    for file in glob.glob(PATH+'*pha*')[0:10]:
        
        ui.load_pha(file)               # Load file
        ui.ignore('0.:0.5,8.0:')        # Set energy range
        ui.subtract()                   # Subtract background
        ui.group_counts(5)              # Bin counts in energy axis
        pdata = ui.get_data_plot()      # Get the object with the spectral bins
        ener_bin_lo.append(pdata.xlo)   
        ener_bin_hi.append(pdata.xhi)
        ener_bin_mid.append(pdata.x)
        fluxes.append(pdata.y)
        errors.append(pdata.yerr)
        src_name = catalog['name'][(catalog['obsid'] == int(file.strip().split('/')[-1][0:24].strip().split('_')[0][-5:])) &
               (catalog['obi'] == int(file.strip().split('/')[-1][0:24].strip().split('_')[1][0:3])) &
               (catalog['region_id'] == int(file.strip().split('/')[-1][0:24].strip().split('_')[2][-4:]))]
        targetids.append(src_name.data[0])
        obs_id.append(int(file.strip().split('/')[-1][0:24].strip().split('_')[0][-5:]))
        
    # Return the results
    return {'name': targetids,
            'obsid': obs_id,
            'spectrum_ene_lo': ener_bin_lo, 
            'spectrum_ene_hi': ener_bin_hi, 
            'spectrum_ene': ener_bin_mid,
            'spectrum_flux': fluxes,
            'spectrum_flux_err': errors}


def save_in_standard_format(args):
    """ Save the spectra in standard HDF5 format
    """
    catalog, output_filename, chandra_data_path = args
    # Create the output directory if it does not exist
    #if not os.path.exists(os.path.dirname(output_filename)):
    #    os.makedirs(os.path.dirname(output_filename))

    # Rename columns to match the standard format
    #catalog['object_id'] = catalog['name']
    #catalog['observation'] = catalog['obsid']
    
    # Process all files
    spectra = Table(processing_fn([catalog,chandra_data_path]))
    catalog = Table(catalog)
    
    catalog['name'] = catalog['name'].astype(str)
    catalog['obsid'] = catalog['obsid'].astype(int)
    spectra['name'] = spectra['name'].astype(str)
    spectra['obsid'] = spectra['obsid'].astype(int)
    # Join on target id with the input catalog
    catalog = join(catalog, spectra, keys=['name','obsid'], join_type='inner')
    
    with h5py.File(output_filename, 'w') as hdf5_file:
        for key in catalog.colnames:
            # Check if the column data type is a string
            if catalog[key].dtype.kind in ['U', 'S']:
                # Encode Unicode string to byte string
                encoded_strings = np.char.encode(catalog[key], 'utf-8')
                hdf5_file.create_dataset(key, data=encoded_strings)
            elif catalog[key].dtype == 'O':  # Column with object dtype, potentially variable-length arrays
                dt = h5py.special_dtype(vlen=np.dtype('float64'))  # or float32 if more appropriate
                dataset = hdf5_file.create_dataset(key, (len(catalog[key]),), dtype=dt)
                for i, item in enumerate(catalog[key]):
                    dataset[i] = item  # Assign the variable-length array directly
            else:
                # Directly save the column as a dataset for non-string types
                hdf5_file.create_dataset(key, data=catalog[key])                
    return 1


def main(args):
    # Load the catalog from the Chandra server, process the spectra, and save
    # the HDF5 file
    
    output_file, PATH = args

    # To donwload the catalog table that associates X-ray sources 
    # (and sky coordinates) to each detection, and therefore to
    # the data products, use the following:

    # CSC 2.0 TAP service
    tap = vo.dal.TAPService('https://cda.cfa.harvard.edu/csc21_snapshot_tap') # For CSC 2.1

    qry = """
    SELECT m.name, m.ra, m.dec, o.obsid, o.obi, o.region_id, o.src_cnts_aper_b,
    o.flux_significance_b, o.flux_aper_b, o.theta, o.flux_bb_aper_b,
    o.gti_mjd_obs, o.hard_hm,o.hard_hs, o.hard_ms, o.var_prob_b, 
    o.var_index_b 
    FROM csc21_snapshot.master_source m, csc21_snapshot.master_stack_assoc a, csc21_snapshot.observation_source o, 
    csc21_snapshot.stack_observation_assoc b, csc21_snapshot.stack_source s 
    WHERE ((a.match_type = 'u') AND (o.flux_bb_aper_b IS NOT NULL) 
    AND (o.src_cnts_aper_b > 50) AND (o.flux_significance_b > 5) 
    AND (o.theta < 5)) AND (m.name = a.name) 
    AND (s.detect_stack_id = a.detect_stack_id and s.region_id = a.region_id) 
    AND (s.detect_stack_id = b.detect_stack_id and s.region_id = b.region_id) 
    AND (o.obsid = b.obsid and o.obi = b.obi and o.region_id = b.region_id)
    ORDER BY name ASC
    """

    cat = tap.search(qry)

    # Conver the catalog to an astropy Table format.
    cat = cat.to_table()

    # Generate HDF5 file
    data_hdf5 = save_in_standard_format([cat,output_file,PATH])

main(args)

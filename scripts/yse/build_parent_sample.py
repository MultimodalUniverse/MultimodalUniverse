import os
import argparse

import h5py
import numpy as np
import sncosmo
import glob

#N       = # number of objects
#t_max   = # maximum number of time for a given object
#N_bands = # number of bands

file_paths = glob.glob(r'../../data/yse_dr1_zenodo/*.dat')


object_id = []
ra = []
dec = []
time = []
flux = []
flux_err = []
band = []
quality_mask = []

redshift = []
host_log_mass = []
spec_class = []

lengths = []

for file_path in file_paths:
    metadata, data = sncosmo.read_snana_ascii(file_path, default_tablename='OBS')
    
    object_id.append(metadata['SNID'])
    ra.append(metadata['RA'])
    dec.append(metadata['DECL'])
    
    redshift.append(metadata['REDSHIFT_FINAL'])
    host_log_mass.append(metadata['HOST_LOGMASS'])
    spec_class.append(metadata['SPEC_CLASS'])

    time.append(data['OBS']['MJD'])
    flux.append(data['OBS']['FLUXCAL'])
    flux_err.append(data['OBS']['FLUXCALERR'])
    band.append(data['OBS']['FLT'])  # TODO: convert to int
    quality_mask.append(data['OBS']['FLAG'])  # TODO: convert to sensible

    lengths.append(len(data['OBS']['MJD']))




if False:
    def main(args):
        # Create a new HDF5 file
        with h5py.File('example.hdf5', 'w') as f:

            dataset1 = f.create_dataset('object_id', data= , dtype=str)
            dataset2 = f.create_dataset('ra'       , data= , dtype=np.float32)
            dataset3 = f.create_dataset('dec'      , data= , dtype=np.float32)


            dataset4 = f.create_dataset('time'        , data= , dtype=np.float32)
            dataset5 = f.create_dataset('flux'        , data= , dtype=np.float32)
            dataset6 = f.create_dataset('flux_err'    , data= , dtype=np.float32)
            dataset7 = f.create_dataset('band'        , data= , dtype=np.int8)
            dataset8 = f.create_dataset('quality_mask', data= , dtype=np.float32)

    if __name__ == '__main__':
        parser = argparse.ArgumentParser(description='Build a parent sample')
        args = parser.parse_args()
        main(args)
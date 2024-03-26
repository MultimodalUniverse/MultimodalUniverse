import os
import argparse

import h5py
import numpy as np

N       = # number of objects
t_max   = # maximum number of time for a given object
N_bands = # number of bands


def padding()



def main(args):
    # Create a new HDF5 file
    with h5py.File('example.hdf5', 'w') as f:

        dataset1 = f.create_dataset('object_id', data= , dtype=np.int32)
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
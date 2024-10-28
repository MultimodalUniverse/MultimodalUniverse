#!/bin/bash

# Need a batcher for downloading the data from MAST
if python build_parent_sample.py -s 23 tglc_data_path='./tglc_data' hdf5_output_path='./tglc_data/s0023/MultimodalUniverse' fits_output_path='./tglc_data/s0023/fits_lcs' --n_processes 4 --tiny; then
    echo "Build parent sample for TESS successful"
else
    echo "Build parent sample  for TESS failed"
    exit 1
fi

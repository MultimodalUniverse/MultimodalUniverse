#!/bin/bash

if python build_parent_sample.py -s 23 --tglc_data_path './tglc' --hdf5_output_path './tglc' --fits_output_path './tglc/fits' --n_processes 4 --tiny; then
    echo "Build for the TGLC parent sample was successful"
else
    echo "Build parent sample for TGLC failed"
    exit 1
fi
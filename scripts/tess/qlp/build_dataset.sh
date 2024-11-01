#!/bin/bash
if python build_parent_sample.py -s 23 --qlp_data_path './qlp' --hdf5_output_path './qlp' --fits_output_path './qlp/fits' --n_processes 4 --tiny; then
    echo "Build for the QLP parent sample was successful"
else
    echo "Build parent sample for QLP failed"
    exit 1
fi

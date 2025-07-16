#!/bin/bash

if python download.py --tiny; then
    echo "download for GALAH successful."
else
    echo "Download for GALAH failed."
    exit 1
fi

cd data/spectra

if ../../prepare.sh; then
    echo "Data preparation (prepare.sh) for GALAH successful"
else
    echo "Data preparation (prepare.sh) for GALAH failed"
    exit 1
fi

cd ../..

if python build_parent_sample.py --data_dir data/spectra/spectra --allstar_file data/GALAH_DR3_main_allstar_v2.fits --resolution_map_dir data --vac_file data/GALAH_DR3_VAC_ages_v2.fits  --missing_id_file data/GALAH_DR3_list_missing_reduced_spectra_v2.csv --output_dir ./dr3 --nside 16 --num_workers 4 --tiny; then
    echo "Build parent sample for GALAH successful"
else
    echo "Build parent sample for GALAH failed"
    exit 1
fi

python test_load.py

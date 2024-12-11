#!/bin/bash

echo DOWNLOADING VAC

aria2c -j16 -s16 -x16 -c -Z "https://cloud.datacentral.org.au/teamdata/GALAH/public/GALAH_DR3/GALAH_DR3_VAC_ages_v2.fits"

echo DOWNLOADING CATALOG
aria2c -j16 -s16 -x16 -c "https://cloud.datacentral.org.au/teamdata/GALAH/public/GALAH_DR3/GALAH_DR3_main_allstar_v2.fits"

echo DOWNLOADING RESOLUTION MAPS

aria2c -j16 -s16 -x16 -c -Z "https://github.com/svenbuder/GALAH_DR3/raw/refs/heads/master/analysis/resolution_maps/ccd1_piv.fits" "https://github.com/svenbuder/GALAH_DR3/raw/refs/heads/master/analysis/resolution_maps/ccd2_piv.fits" "https://github.com/svenbuder/GALAH_DR3/raw/refs/heads/master/analysis/resolution_maps/ccd3_piv.fits" "https://github.com/svenbuder/GALAH_DR3/raw/refs/heads/master/analysis/resolution_maps/ccd4_piv.fits"

echo DOWNLOADING SPECTRA

mkdir spectra
aria2c -j16 -s16 -x16 -c -Z "https://cloud.datacentral.org.au/teamdata/GALAH/public/GALAH_DR3/spectra/tar_files/151224_com.tar.gz" "https://cloud.datacentral.org.au/teamdata/GALAH/public/GALAH_DR3/spectra/tar_files/151224_com2.tar.gz" --dir=spectra

echo DOWNLOADING MISSING SPECTRA LIST

aria2c -j16 -s16 -x16 -c "https://cloud.datacentral.org.au/teamdata/GALAH/public/GALAH_DR3/spectra/GALAH_DR3_list_missing_reduced_spectra_v2.csv"

#!/bin/bash

mkdir -p spectra

# extract tar files
find . -type f -name "*.tar.gz" -exec tar -zxvf {} \;

# flatten spectra
find . -type f -name "*.fits" -exec mv {} spectra \;

# remove non spectra
find . -type f ! -name "*.fits" ! -name "*.tar.gz" -exec rm {} \;

# clean up empty folders
find . -depth -type d -empty -delete

#!/bin/bash

mkdir -p spectra

echo extracting tar files
find . -type f -name "*.tar.gz" -exec tar -zxvf {} \;

echo flattening spectra files
find . -type f -name "*.fits" -exec mv {} spectra \;

echo removing non spectra files
find . -type f ! -name "*.fits" ! -name "*.tar.gz" -exec rm {} \;

echo cleaning up empty folders
find . -depth -type d -empty -delete

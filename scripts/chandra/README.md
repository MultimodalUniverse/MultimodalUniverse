# Download Chandra X-ray spectral Data

This dataset consists of reduced X-ray spectra for sources in the [Chandra Source Catalog](https://cxc.cfa.harvard.edu/csc/), 
which is the repository of all sources detected by the Chandra X-ray Observatory. The catalog contains many data files,
and here we will be adding only the spectra. Spectral files consist of three types of files: the Pulse Height Amplitude (PHA)
file, which records the charge per pixel in the CCD, and two spectral response files the ARF, and the RMF, that together
account for the telescopes effective area as a function of energy, quantum efficiency, etc. The scripts here will download the
three types of files, processs the PHA files using the response files, and produce the final spectra as a loadable dataset.

## Downloading the data

The script [download_script.py](./download_script.py) downloads the spectral files directly from the Chandra server, using the [CLI protocol](https://cxc.cfa.harvard.edu/csc/cli/).
Here is how to use it:

```shell
python download_script.py \
  --min_cnts $MINIMUM_NUMBER_OF_SOURCE_COUNTS \
  --min_sig $MINIMUM_SIGNAL_TO_NOISE_RATIO \
  --max_theta $MAXIMUM_OFF_AXIS_ANGLE \
  --output_file $NAME_OF_FILE \
  --file_path $OUTPUT_DIR
```

The first three arguments are the number of X-ray counts in the source detection, the signal to noise of the detection,
and the angle from the center of the field (the PSF degrades in a X-ray telescope far from the center of the field).
The lower the number of counts or the signal to noise, the more source spectra are dowloaded, because dimmer sources
are also selected. Recommended values for a short test are: 4000, 80, 1. To download the largest set of recommended sources is the default: 40, 4, 10.

`$NAME_OF_FILE` refers to the catalog file that is created.


## Installing dependencies

In order to process the spectrum using the response files, the [Sherpa](https://cxc.cfa.harvard.edu/sherpa/) software is required. The installation can be verified with:

`pip show sherpa`

and installed if needed with:
`pip install sherpa`

## Processing the spectrum
The spectral files are processed to produce the spectra, and saved to the hdf5 file with:


```
```shell
python build_parent_sample.py \
  --cat_file $CATALOG_FILE \
  --output_file $NAME_OF_HDF5_FILE \
  --file_path $PATH_TO_SPECTRAL_FILES
```


Where the catalog file is the one created by [download_script.py](./download_script.py), `$NAME_OF_HDF5_FILE` is the name where the dataset will be stored, and the path refers to the directory where the spectral files have been previously downloaded.



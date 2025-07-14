# LAMOST Dataset Collection

This folder contains the scripts and queries used to build the LAMOST spectroscopic parent sample, based on optical spectra from the Large Sky Area Multi-Object Fiber Spectroscopic Telescope (LAMOST).

## Sample selection

In the current version of the dataset, we retrieve optical spectra from LAMOST. The following cuts are applied:

```
    - snr > 20                 # Minimum signal-to-noise ratio
    - mag_r < 19.0             # Magnitude limit for reliable spectra
    - class != "UNKNOWN"       # Exclude objects with unknown classification
    - rv_flag = 0              # Only include objects with reliable radial velocity
```

These cuts ensure high-quality spectroscopic data suitable for multimodal analysis.

## Data preparation

The data can be loaded for example as follows:
```python
from datasets import load_dataset
import matplotlib.pyplot as plt

dataset = load_dataset('./lamost.py', trust_remote_code=True, split='train')
spectrum = dataset['train'][0]['spectrum']
spectrum.keys()

plt.plot(spectrum['lambda'], spectrum['flux'], color='royalblue')
plt.xlabel(r'wavelength ($\AA$)')
plt.ylabel(r'flux (normalized)')
plt.title('LAMOST Optical Spectrum')
plt.show()
```

### Downloading data

The LAMOST data can be downloaded from the official LAMOST data release website or via the built-in download functionality in the scripts.

### Spectra extraction

Once the LAMOST data has been downloaded, you can create the parent sample by running the following script:

```bash
python build_parent_sample.py [path to LAMOST data] [output directory] --num_processes [number of processes]
```

e.g. `python build_parent_sample.py /data/lamost/dr9 /data/MultimodalUniverse/lamost --num_processes 40`

If there is no LAMOST data downloaded in the location provided, it will be downloaded by the script automatically.

### Test the dataset

You can test the dataset creation with a small sample by running:

```bash
bash test.sh
```

### Documentation

- LAMOST official website: http://www.lamost.org/
- LAMOST data release: http://dr9.lamost.org/
- LAMOST survey overview: https://ui.adsabs.harvard.edu/abs/2012RAA....12.1197C/abstract
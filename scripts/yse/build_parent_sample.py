# %%
import h5py
import numpy as np
import sncosmo
import glob

# %%
def get_str_dtype(arr):
    str_max_len = int(np.char.str_len(arr).max())
    return h5py.string_dtype(encoding='utf-8', length=str_max_len)

def convert_dtype(arr):
    if np.issubdtype(arr.dtype, np.floating):
        dtype = np.float32
    elif np.issubdtype(arr.dtype, np.str_):
        dtype = get_str_dtype(arr)
    else:
        dtype = arr.dtype
    return arr.astype(dtype)

# %%
file_paths = glob.glob(r'../../data/yse_dr1_zenodo/*.dat')
num_examples = len(file_paths)

# %%
"""
ignored_keys = {'PARSNIP_S1', 'PARSNIP_S2', 'PARSNIP_S3', 'SET_ZTF_FP', 'processing', 'SEARCH_PEAKFLT', 'END'}

example_metadata, example_data = sncosmo.read_snana_ascii(file_paths[0], default_tablename='OBS')
example_data = example_data['OBS']
key_metadata = [key for key in example_metadata.keys() if key not in ignored_keys]
#key_metadata = list(example_metadata.keys())
key_data = [key for key in example_data.keys() if key not in ignored_keys]
#key_data = example_data.keys()
key_all = key_metadata + key_data
name_conversion = dict(zip(key_all, key_all))
name_conversion.update({
    'SNID': 'object_id',
    'RA': 'ra',
    'DECL': 'dec',
    'MJD': 'time',
    'FLUXCAL': 'flux',
    'FLUXCALERR': 'flux_err',
    'FLT': 'band',
    'FLAG': 'quality_mask',
})
field_data = {name_conversion[key] for key in key_data}
field_metadata = {name_conversion[key] for key in key_metadata}
"""

field_data = ['time', 'flux', 'flux_err', 'band', 'quality_mask',]
key_data = ['MJD', 'FLUXCAL', 'FLUXCALERR', 'FLT', 'FLAG',]

field_metadata = ['object_id', 'ra', 'dec',  'redshift', 'host_log_mass', 'spec_class']
key_metadata = ['SNID', 'RA', 'DECL', 'REDSHIFT_FINAL', 'HOST_LOGMASS', 'SPEC_CLASS']

# %%
data = dict(zip(field_data, ([] for _ in field_data)))
metadata = dict(zip(field_metadata, ([] for _ in field_metadata)))

for file_path in file_paths:
    metadata_, data_ = sncosmo.read_snana_ascii(file_path, default_tablename='OBS')
    data_ = data_['OBS']
    for field, key in zip(field_data, key_data):
        data[field].append(data_[key].data)
        # the data are astropy columns wrapping numpy arrays which are accessed via .data
    for field, key in zip(field_metadata, key_metadata):
        metadata[field].append(metadata_[key])

# %%
all_bands = np.unique(np.concatenate(data['band']))

# %%
max_length = 0
for i in range(num_examples):
    band, count = np.unique(data['band'][i], return_counts=True)
    max_length = max(max_length, count.max())

# %%
field_data.remove('band')
banded_data = dict(zip(field_data, ([] for _ in field_data)))

for i in range(num_examples):
    mask = np.expand_dims(all_bands, 1) == data['band'][i]
    for field in field_data:
        d = []
        for j in range(len(all_bands)):
            d_ = data[field][i][mask[j]]
            d_ = np.pad(d_, (0, max_length - len(d_)), mode='constant', constant_values=np.nan)
            d.append(d_)
        banded_data[field].append(d)

for field in field_data:
    banded_data[field] = np.array(banded_data[field])
banded_data['band'] = np.array((all_bands for _ in range(num_examples)))

# %%
for field in field_metadata:
    metadata[field] = np.array(metadata[field])

# %%
with h5py.File('../../data/yse.hdf5', 'w') as hdf5_file:
    for field in field_metadata:
        hdf5_file.create_dataset(field, data=convert_dtype(metadata[field]))
    for field in field_data:
        hdf5_file.create_dataset(field, data=convert_dtype(banded_data[field]))

# %%




import glob
import pickle

import h5py
import numpy as np
import pandas as pd


# %%
def get_str_dtype(arr):
    str_max_len = int(np.char.str_len(arr).max())
    return h5py.string_dtype(encoding="utf-8", length=str_max_len)


def convert_dtype(arr):
    if np.issubdtype(arr.dtype, np.floating):
        dtype = np.float32
    elif np.issubdtype(arr.dtype, np.str_):
        dtype = get_str_dtype(arr)
    else:
        dtype = arr.dtype
    return arr.astype(dtype)


# %%
field_data = ["time", "mag", "mag_err", "band"]
field_metadata = ["object_id", "ra", "dec"]
data = dict(zip(field_data, ([] for _ in field_data)))
metadata = dict(zip(field_metadata, ([] for _ in field_metadata)))

data_ = dict(zip(field_data, ([] for _ in field_data)))
info = {}
with open("../../scripts/cfa/info.dat", "r") as f:
    for line in f.readlines():
        info["SN20" + line.split()[0]] = line.split()[1:]
optical_df = pd.read_csv(
    f"../../data/CFA_SNII/STDSYSTEM_LC.txt",
    comment="#",
    delim_whitespace=True,
    names=["Name", "band", "time", "N", "mag", "mag_err"],
)
nir_df = pd.read_csv(
    f"../../data/CFA_SNII/NIR_LC.txt",
    comment="#",
    delim_whitespace=True,
    names=["Name", "band", "time", "mag", "mag_err"],
)
df = pd.concat([optical_df, nir_df])
for sn_name in set(df["Name"]):
    mask = np.where(df["Name"] == sn_name)
    for key in field_data:
        data[key].append(np.array(df[key])[mask])
    metadata["object_id"].append(sn_name)
    metadata["ra"].append(float(info[sn_name][0]))
    metadata["dec"].append(float(info[sn_name][1]))

# %%
all_bands = np.unique(np.concatenate(data["band"]))

# %%
num_examples = len(info.keys())
max_length = 0
for i in range(num_examples):
    band, count = np.unique(data["band"][i], return_counts=True)
    max_length = max(max_length, count.max())

# %%
field_data.remove("band")
banded_data = dict(zip(field_data, ([] for _ in field_data)))

for i in range(num_examples):
    mask = np.expand_dims(all_bands, 1) == data["band"][i]
    for field in field_data:
        d = []
        for j in range(len(all_bands)):
            d_ = np.array(data[field][i])[mask[j]]
            d_ = np.pad(
                d_, (0, max_length - len(d_)), mode="constant", constant_values=np.nan
            )
            d.append(d_)
        banded_data[field].append(d)

for field in field_data:
    banded_data[field] = np.array(banded_data[field])
banded_data["band"] = np.array((all_bands for _ in range(num_examples)))

# %%
for field in field_metadata:
    metadata[field] = np.array(metadata[field])

# %%
with h5py.File("../../data/CFA_SNII.hdf5", "w") as hdf5_file:
    for field in field_metadata:
        hdf5_file.create_dataset(field, data=convert_dtype(metadata[field]))
    for field in field_data:
        hdf5_file.create_dataset(field, data=convert_dtype(banded_data[field]))

# %%

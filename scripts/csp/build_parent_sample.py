import glob

import h5py
import numpy as np


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
file_paths = glob.glob(r"../../data/CSPDR3/*snpy.txt")
num_examples = len(file_paths)

# %%
field_data = ["time", "mag", "mag_err", "band"]
field_metadata = ["object_id", "redshift", "ra", "dec"]
data = dict(zip(field_data, ([] for _ in field_data)))
metadata = dict(zip(field_metadata, ([] for _ in field_metadata)))

for file_path in file_paths:
    current_filter = None
    data_ = dict(zip(field_data, ([] for _ in field_data)))
    f = open(file_path, "r")
    for i, line in enumerate(f.readlines()):
        if i == 0:
            for key, val in zip(field_metadata, line.split()):
                if key in ("ra", "dec"):
                    val = float(val)
                metadata[key].append(val)
            continue
        if line.startswith("filter"):
            current_filter = line.split()[1]
            continue
        for key, val in zip(field_data[:-1], line.split()):
            data_[key].append(float(val))
        data_["band"].append(current_filter)
    for key in field_data:
        data[key].append(data_[key])
    f.close()

# %%
all_bands = np.unique(np.concatenate(data["band"]))

# %%
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
with h5py.File("../../data/CSPDR3.hdf5", "w") as hdf5_file:
    for field in field_metadata:
        hdf5_file.create_dataset(field, data=convert_dtype(metadata[field]))
    for field in field_data:
        hdf5_file.create_dataset(field, data=convert_dtype(banded_data[field]))

# %%

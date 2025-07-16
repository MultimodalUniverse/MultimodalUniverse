import datasets

ds_hdf5 = datasets.load_dataset(
    "twomass.py",
    trust_remote_code=True,
    split="train",
    streaming=True,
)

batch_hdf5 = next(iter(ds_hdf5))

print(batch_hdf5["object_id"])
print(batch_hdf5["ra"])
print(batch_hdf5["dec"])

ds_parquet = datasets.load_dataset(
    "_2mass_pq",
    trust_remote_code=True,
    split="train",
    streaming=True,
)

batch_parquet = next(iter(ds_parquet))

import datasets

ds = datasets.load_dataset("galex.py", trust_remote_code=True, split="train", streaming=True).with_format("numpy")

batch = next(iter(ds))

print(batch.keys())
print(batch['object_id'])
print(batch['nuv_mag'])
print(batch['fuv_mag'])
print(batch['nuv_magerr'])
print(batch['fuv_magerr'])

print("Done!")

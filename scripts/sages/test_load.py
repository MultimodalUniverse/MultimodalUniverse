import datasets

ds = datasets.load_dataset("sages.py", trust_remote_code=True, split="train", streaming=True).with_format("numpy")

batch = next(iter(ds))

print(batch.keys())
print(batch['object_id'])
print(batch['MAG_U'])
print(batch['MAG_V'])

print("Done!")


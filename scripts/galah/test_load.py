import datasets

ds = datasets.load_dataset(
    "./galah.py",
    trust_remote_code=True,
    split="train",
    streaming=True,
).with_format("numpy")

batch = next(iter(ds))

print("keys: ", list(batch.keys()))
print("spectrum keys:", list(batch["spectrum"].keys()))

print("done loading")

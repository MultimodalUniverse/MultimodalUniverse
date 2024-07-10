import datasets


for k in ["dr3_source", "dr3_rvs", "dr3_xp"]:
    ds = datasets.load_dataset(
        "./gaia.py",
        k,
        trust_remote_code=True,
        split="train",
        streaming=True,
    ).with_format("numpy")

    batch = next(iter(ds))

    print(f"loaded {k} batch!")




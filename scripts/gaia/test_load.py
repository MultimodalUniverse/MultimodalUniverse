import datasets


ds = datasets.load_dataset(
    "./gaia.py",
    trust_remote_code=True,
    split="train",
    streaming=True,
).with_format("numpy")

batch = next(iter(ds))

assert batch["spectral_coefficients"]["coeff"].size == 110
assert batch["spectral_coefficients"]["coeff_error"].size == 110

for k in ["ra", "dec", "pmra", "pmdec", "parallax"]:
    assert k in batch["astrometry"].keys()

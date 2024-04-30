import datasets


ds = datasets.load_dataset(
    "./gaia.py",
    trust_remote_code=True,
    split="train",
    streaming=True,
).with_format("numpy")

batch = next(iter(ds))

print("testing spectral coefficients")

assert batch["spectral_coefficients"]["coeff"].size == 110
assert batch["spectral_coefficients"]["coeff_error"].size == 110

print("testing astrometry")

for k in ["ra", "dec", "pmra", "pmdec", "parallax"]:
    assert k in batch["astrometry"].keys()

print("testing photometry")

for k in ["bp_rp", "phot_g_mean_mag", "phot_bp_mean_mag", "phot_rp_mean_mag"]:
    assert k in batch["photometry"].keys()

print("Test passed!")

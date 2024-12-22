from datasets import load_dataset
import numpy as np

def test_dataset():
    # You can use the datasets dataset as a test dataset
    dset = load_dataset('./kepler.py', trust_remote_code=True, split='train', streaming='true').with_format('numpy')
    print(dset)
    for i, data in enumerate(dset):
        pdcsap_flux = data['lightcurve']['pdcsap_flux']
        sap_flux = data['lightcurve']['sap_flux']
        pdcsap_nan = np.isnan(pdcsap_flux).sum()
        sap_nan = np.isnan(sap_flux).sum()
        print(pdcsap_flux.shape, pdcsap_nan, sap_flux.shape, sap_nan)
        if i > 10:
            break


if __name__ == '__main__':
    test_dataset()
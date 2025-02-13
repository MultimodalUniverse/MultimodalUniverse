from datasets import load_dataset
import numpy as np
import matplotlib.pyplot as plt

def test_dataset():
    dset = load_dataset('./kepler.py', trust_remote_code=True, split='train', streaming='true').with_format('numpy')
    # print(dset.info)
    print(next(iter(dset)))
    for i, data in enumerate(dset):
        pdcsap_flux = data['lightcurve']['pdcsap_flux']
        sap_flux = data['lightcurve']['sap_flux']
        pdcsap_err = data['lightcurve']['pdcsap_flux_err']
        sap_err = data['lightcurve']['sap_flux_err']
        time = data['lightcurve']['time']
        fig, ax = plt.subplots(1, 2)
        ax[0].errorbar(time, pdcsap_flux, yerr=pdcsap_err,)
        ax[1].errorbar(time, sap_flux, yerr=sap_err)
        plt.tight_layout()
        plt.savefig(f'figs/{i}.png')
        plt.close()
        print(np.nanmax(pdcsap_flux), np.nanmax(pdcsap_err), np.nanmax(sap_flux), np.nanmax(sap_err))
        if i > 10:
            break


if __name__ == '__main__':
    test_dataset()
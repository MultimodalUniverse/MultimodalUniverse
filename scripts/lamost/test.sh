#!/bin/bash

CATALOG=LRS_galaxy # also try LRS_cv

python3 download_data.py --catalog_name ${CATALOG} --release dr10_v2.0 -o ./${CATALOG,,}_spectra -i 50 -n 8 -d 1.0 --timeout 30 --retries 3
python3 build_parent_sample.py dr10_v2.0_${CATALOG}.fits ./${CATALOG,,}_spectra/ . --tiny

python3 -c "
try:
    from datasets import load_dataset

    ds = load_dataset('lamost.py', name='dr10_v20_${CATALOG,,}', trust_remote_code=True, split='train').with_format('pytorch')

    batch = ds[0]

    print(f'Loaded sample keys: {list(batch.keys())}')

    print('flux shape:', batch['spectrum_flux'].shape, ' wavelength shape:', batch['spectrum_wavelength'].shape)

except Exception:
    raise RuntimeError('Failed to load the dataset.')
"

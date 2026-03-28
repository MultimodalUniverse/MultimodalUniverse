#!/bin/bash

if python3 build_parent_sample.py --metadata_path=metadata_main.csv --morphology_path=gz_hubble_main.csv --downloads_folder=COSMOS/ --output_dir=COSMOS/ --debug=True --nan_tolerance=0.2 --zero_tolerance=0.2; then
    echo "Build tiny parent sample successful"
else
    echo "Build tiny parent sample failed"
fi

if python -c "from datasets import load_dataset; dset = load_dataset('./hst_cosmos.py', 'hst-cosmos', trust_remote_code=True, split='train', streaming=True).with_format('numpy'); print(next(iter(dset)))"; then
    echo "Load tiny dataset successful"
else
    echo "Load tiny dataset failed"
fi


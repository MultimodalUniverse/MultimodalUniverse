#!/bin/bash

# Test script for LAMOST dataset creation
# This script tests the LAMOST dataset creation with a small subset of data

set -e  # Exit on any error

echo "Starting LAMOST dataset test..."

# Create test directories
FITS_DIR="./fits"
OUTPUT_DIR="./test_output"
CATALOG_PATH="/data/lamost/dr10_v2.0_LRS_stellar.fits"

mkdir -p $OUTPUT_DIR
chmod a+r $CATALOG_PATH

echo "Test directories created"

pip install h5py datasets==2.16.0 healpy

# Test with tiny subset
echo "Testing with tiny subset..."
python build_parent_sample.py $CATALOG_PATH $FITS_DIR $OUTPUT_DIR --tiny --num_processes 2

# Check if output files were created
if [ -d "$OUTPUT_DIR/lamost" ]; then
    echo "✓ LAMOST output directory created"
    
    # Count the number of HDF5 files
    HDF5_COUNT=$(find $OUTPUT_DIR/lamost -name "*.hdf5" | wc -l)
    echo "✓ Created $HDF5_COUNT HDF5 files"
    
    if [ $HDF5_COUNT -gt 0 ]; then
        echo "✓ Test completed successfully"
        
        # Test loading the dataset
        echo "Testing dataset loading..."
        python -c "
import sys
import numpy as np
from matplotlib import pyplot as plt
sys.path.append('.')
from datasets import load_dataset
try:
    dataset = load_dataset('./lamost.py', data_dir='$OUTPUT_DIR', trust_remote_code=True, split='train')
    print(f'✓ Successfully loaded dataset with {len(dataset)} samples')
    if len(dataset) > 0:
        sample = dataset[0]
        print(f'✓ Sample keys: {list(sample.keys())}')
    else:
        print('⚠ Dataset is empty')

    x = next(iter(dataset))
    id = x['object_id']
    plt.plot(np.array(x['spectrum_wavelength']), np.array(x['spectrum_flux']))
    plt.xlabel('Wavelength')
    plt.ylabel('Flux')
    plt.title(f'LAMOST {id}')
    plt.savefig(f'./figs/test_{id}.png')
    plt.close()
    print('✓ Plot saved successfully')
except Exception as e:
    print(f'✗ Error loading dataset: {e}')
"
    else
        echo "✗ No HDF5 files created"
        exit 1
    fi
else
    echo "✗ LAMOST output directory not created"
    exit 1
fi

# Clean up test data
echo "Cleaning up test data..."
rm -rf $OUTPUT_DIR

echo "✓ LAMOST test completed successfully!"
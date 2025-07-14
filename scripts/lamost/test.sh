#!/bin/bash

# Test script for LAMOST dataset creation
# This script tests the LAMOST dataset creation with a small subset of data

set -e  # Exit on any error

echo "Starting LAMOST dataset test..."

# Create test directories
TEST_DIR="./test_data"
OUTPUT_DIR="./test_output"

mkdir -p $TEST_DIR
mkdir -p $OUTPUT_DIR

echo "Test directories created"

# Test with tiny subset
echo "Testing with tiny subset..."
python build_parent_sample.py $TEST_DIR $OUTPUT_DIR --tiny --num_processes 2

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
sys.path.append('.')
from datasets import load_dataset
try:
    dataset = load_dataset('./lamost.py', data_dir='$OUTPUT_DIR', trust_remote_code=True, split='train')
    print(f'✓ Successfully loaded dataset with {len(dataset)} samples')
    if len(dataset) > 0:
        sample = dataset[0]
        print(f'✓ Sample keys: {list(sample.keys())}')
        print(f'✓ Spectrum shape: {sample[\"spectrum\"][\"flux\"].shape}')
    else:
        print('⚠ Dataset is empty')
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
rm -rf $TEST_DIR $OUTPUT_DIR

echo "✓ LAMOST test completed successfully!"
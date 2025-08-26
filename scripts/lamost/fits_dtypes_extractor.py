#!/usr/bin/env python3
"""
Script to extract column dtypes from a FITS table file in HuggingFace-compatible format.

Usage:
    python fits_dtypes_extractor.py <fits_file_path>
"""

import sys
import argparse
from pathlib import Path
from astropy.table import Table
import numpy as np


def numpy_to_hf_dtype(numpy_dtype):
    """
    Convert numpy dtype to HuggingFace-compatible dtype string.
    
    Args:
        numpy_dtype: numpy dtype object
        
    Returns:
        str: HuggingFace-compatible dtype string
    """
    dtype_str = str(numpy_dtype)
    
    # Handle string types
    if numpy_dtype.char in ('U', 'S') or 'str' in dtype_str or 'unicode' in dtype_str:
        return 'string'
    
    # Handle boolean
    if numpy_dtype == np.bool_ or 'bool' in dtype_str:
        return 'bool'
    
    # Handle integer types
    if np.issubdtype(numpy_dtype, np.integer):
        if numpy_dtype == np.int8:
            return 'int8'
        elif numpy_dtype == np.int16:
            return 'int16'
        elif numpy_dtype == np.int32:
            return 'int32'
        elif numpy_dtype == np.int64:
            return 'int64'
        elif numpy_dtype == np.uint8:
            return 'uint8'
        elif numpy_dtype == np.uint16:
            return 'uint16'
        elif numpy_dtype == np.uint32:
            return 'uint32'
        elif numpy_dtype == np.uint64:
            return 'uint64'
        else:
            # Default to int64 for other integer types
            return 'int64'
    
    # Handle floating point types
    if np.issubdtype(numpy_dtype, np.floating):
        if numpy_dtype == np.float16:
            return 'float16'
        elif numpy_dtype == np.float32:
            return 'float32'
        elif numpy_dtype == np.float64:
            return 'float64'
        else:
            # Default to float32 for other float types
            return 'float32'
    
    # Handle complex types (map to float since HF doesn't have native complex support)
    if np.issubdtype(numpy_dtype, np.complexfloating):
        if numpy_dtype == np.complex64:
            return 'float32'  # Each component is float32
        else:
            return 'float64'  # Each component is float64
    
    # Default fallback
    return 'string'


def extract_dtypes_from_fits(fits_file_path):
    """
    Extract column dtypes from a FITS table file.
    
    Args:
        fits_file_path (str): Path to the FITS file
        
    Returns:
        dict: Dictionary mapping column names to HuggingFace-compatible dtype strings
    """
    try:
        # Read the FITS table
        table = Table.read(fits_file_path)
        
        # Extract dtypes for each column
        dtypes_dict = {}
        for col_name in table.colnames:
            numpy_dtype = table[col_name].dtype
            hf_dtype = numpy_to_hf_dtype(numpy_dtype)
            dtypes_dict[col_name] = hf_dtype
        
        return dtypes_dict
        
    except Exception as e:
        print(f"Error reading FITS file: {e}", file=sys.stderr)
        return None


def main():
    parser = argparse.ArgumentParser(
        description="Extract column dtypes from a FITS table file in HuggingFace-compatible format"
    )
    parser.add_argument(
        "fits_file",
        help="Path to the FITS file containing a table"
    )
    parser.add_argument(
        "--output", "-o",
        help="Output file to save the dtypes dictionary (optional, prints to stdout by default)"
    )
    parser.add_argument(
        "--format", "-f",
        choices=["dict", "json"],
        default="dict",
        help="Output format: 'dict' for Python dict representation, 'json' for JSON format"
    )
    
    args = parser.parse_args()
    
    # Check if file exists
    fits_path = Path(args.fits_file)
    if not fits_path.exists():
        print(f"Error: File '{fits_path}' does not exist.", file=sys.stderr)
        return 1
    
    # Extract dtypes
    dtypes_dict = extract_dtypes_from_fits(args.fits_file)
    if dtypes_dict is None:
        return 1
    
    # Format output
    if args.format == "json":
        import json
        output_str = json.dumps(dtypes_dict, indent=2)
    else:
        output_str = repr(dtypes_dict)
    
    # Output results
    if args.output:
        with open(args.output, 'w') as f:
            f.write(output_str)
        print(f"Dtypes dictionary saved to: {args.output}")
    else:
        print(output_str)
    
    return 0


if __name__ == "__main__":
    sys.exit(main())

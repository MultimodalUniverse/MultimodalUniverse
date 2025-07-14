#!/usr/bin/env python3
"""
Download LAMOST data files from the official data release website.
This script handles downloading catalog files and provides guidance for spectrum files.

Updated to work with the actual LAMOST data access system.
"""

import argparse
import os
import urllib.request
import urllib.error
import urllib.parse
import time
import logging
from pathlib import Path

import numpy as np
from astropy.table import Table
from tqdm import tqdm

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# LAMOST data release URLs - these are the actual working catalog URLs
LAMOST_URLS = {
    'dr13_v0': {
        'base': 'https://www.lamost.org/dr13/v0/',
        'catalog': 'https://www.lamost.org/dr13/v0/catalogue'
    },
    'dr12_v1': {
        'base': 'https://www.lamost.org/dr12/',
        'catalog': 'https://www.lamost.org/dr12/catalogue'
    },
    'dr11_v1': {
        'base': 'https://www.lamost.org/dr11/',
        'catalog': 'https://www.lamost.org/dr11/catalogue'
    },
    'dr10_v1': {
        'base': 'https://www.lamost.org/dr10/v1.0/',
        'catalog': 'https://www.lamost.org/dr10/v1.0/catalogue'
    },
    'dr9_v2': {
        'base': 'https://www.lamost.org/dr9/v2.0/',
        'catalog': 'https://www.lamost.org/dr9/v2.0/catalogue'
    }
}

# Catalog file mappings - these are the actual catalog files available
# Note: These are template URLs - the actual download links may be different
CATALOG_TYPES = [
    'general',           # General catalog
    'afgk_stars',        # A, F, G, K type stars
    'a_stars',           # A type stars line index catalog  
    'm_stars',           # gM, dM, sdM stars
    'multiple_epoch',    # Multiple epoch catalog
    'plate_info',        # Observed plate information
    'input_catalog',     # Input catalog
    'cv_stars',          # Cataclysmic variable stars
    'white_dwarf',       # White dwarf stars
    'qso_emission',      # QSO emission line features
    'galaxy_synthesis'   # Stellar population synthesis of galaxies
]

def get_catalog_urls(dr_version, catalog_type, file_format):
    """Generate catalog download URLs based on DR version and type"""
    base_url = LAMOST_URLS[dr_version]['base']
    
    # Template filenames based on LAMOST naming conventions
    filename_templates = {
        'dr13_v0': f'dr13_v0_lr_{catalog_type}.{file_format}',
        'dr12_v1': f'dr12_v1_lr_{catalog_type}.{file_format}', 
        'dr11_v1': f'dr11_v1_lr_{catalog_type}.{file_format}',
        'dr10_v1': f'dr10_v1_lr_{catalog_type}.{file_format}',
        'dr9_v2': f'dr9_v2_lr_{catalog_type}.{file_format}'
    }
    
    filename = filename_templates.get(dr_version)
    if filename:
        return f"{base_url}catalogue/{filename}"
    return None

def download_file(url, local_path, max_retries=3):
    """Download a file from URL to local path with retry mechanism"""
    for attempt in range(max_retries):
        try:
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            
            logger.info(f"Downloading {os.path.basename(local_path)}... (attempt {attempt + 1})")
            
            # Add headers to mimic a browser request
            req = urllib.request.Request(url)
            req.add_header('User-Agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
            
            with urllib.request.urlopen(req, timeout=30) as response:
                with open(local_path, 'wb') as f:
                    f.write(response.read())
            
            logger.info(f"Successfully downloaded {os.path.basename(local_path)}")
            return local_path
            
        except Exception as e:
            logger.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)  # Exponential backoff
            else:
                logger.error(f"Failed to download {url} after {max_retries} attempts")
                return None

def download_catalog_files(dr_version, output_dir, file_format='fits'):
    """Download LAMOST catalog files"""
    catalog_dir = Path(output_dir) / "catalogs"
    catalog_dir.mkdir(parents=True, exist_ok=True)
    
    if dr_version not in CATALOG_FILES:
        logger.error(f"Data release {dr_version} not supported. Available: {list(CATALOG_FILES.keys())}")
        return []
    
    catalogs = CATALOG_FILES[dr_version]
    downloaded_files = []
    
    for catalog_name, urls in catalogs.items():
        if file_format not in urls:
            logger.warning(f"Format {file_format} not available for {catalog_name}")
            continue
            
        url = urls[file_format]
        filename = f"{catalog_name}_{dr_version}.{file_format}"
        local_path = catalog_dir / filename
        
        if local_path.exists():
            logger.info(f"{filename} already exists, skipping")
            downloaded_files.append(str(local_path))
            continue
        
        result = download_file(url, str(local_path))
        if result:
            downloaded_files.append(result)
    
    return downloaded_files

def analyze_catalog(catalog_path):
    """Analyze a catalog file to understand its structure"""
    try:
        if catalog_path.endswith('.fits'):
            catalog = Table.read(catalog_path, hdu=1)
        else:  # CSV
            catalog = Table.read(catalog_path, format='csv')
        
        logger.info(f"Catalog {os.path.basename(catalog_path)} contains {len(catalog)} entries")
        logger.info(f"Columns: {catalog.colnames}")
        
        # Look for filename and obsid columns
        filename_cols = [col for col in catalog.colnames if 'filename' in col.lower()]
        obsid_cols = [col for col in catalog.colnames if 'obsid' in col.lower()]
        
        logger.info(f"Filename columns: {filename_cols}")
        logger.info(f"OBSID columns: {obsid_cols}")
        
        if filename_cols:
            sample_files = catalog[filename_cols[0]][:5]
            logger.info(f"Sample filenames: {list(sample_files)}")
        
        return catalog
        
    except Exception as e:
        logger.error(f"Error reading catalog {catalog_path}: {e}")
        return None

def print_spectrum_access_info():
    """Print information about how to access individual spectra"""
    info = """
=== LAMOST SPECTRUM ACCESS INFORMATION ===

LAMOST spectra are not available for direct bulk download via simple URLs.
Instead, you need to use one of these methods:

1. LAMOST Web Interface:
   - Go to https://www.lamost.org/dr9/v2.0/search
   - Use the search interface to find and download individual spectra
   - You can search by coordinates, object names, or catalog parameters

2. Programmatic Access:
   - Use the LAMOST data access API (if available)
   - Contact LAMOST team for bulk access: http://www.lamost.org/contact

3. Spectrum File Structure:
   - Spectra are named: spec-LMJD-PLANID_spXX-FFF.fits
   - LMJD: Local Modified Julian Day
   - PLANID: Plan identifier
   - XX: Spectrograph number (01-16)
   - FFF: Fiber number (001-250)

4. Alternative Data Sources:
   - Check if your institution has access to LAMOST mirror sites
   - Consider using astronomical archives like CDS VizieR
   - Look for processed LAMOST data in other surveys

For large-scale access, it's recommended to:
1. Download the catalogs (which this script does)
2. Filter the catalogs to identify spectra you need
3. Contact LAMOST directly for bulk access arrangements
4. Use the web interface for smaller datasets

The catalog files contain all the metadata you need to identify
specific spectra, including filenames and observation parameters.
"""
    print(info)

def generate_example_usage():
    """Generate example code for using the downloaded catalogs"""
    example_code = '''
# Example: How to use the downloaded LAMOST catalogs

from astropy.table import Table
import numpy as np

# Load a catalog
catalog = Table.read('catalogs/general_dr9_v2.fits')

# Filter by signal-to-noise ratio
high_snr = catalog[catalog['snr_g'] > 50]

# Filter by stellar type
stars = catalog[catalog['class'] == 'STAR']

# Filter by magnitude
bright_stars = catalog[catalog['mag_g'] < 15]

# Get filenames for download
filenames = catalog['filename']
obsids = catalog['obsid']

# Print some statistics
print(f"Total objects: {len(catalog)}")
print(f"High S/N objects: {len(high_snr)}")
print(f"Stars: {len(stars)}")
print(f"Bright stars: {len(bright_stars)}")

# Example spectrum filename from catalog
if len(catalog) > 0:
    example_file = catalog['filename'][0]
    print(f"Example spectrum file: {example_file}")
'''
    
    with open('lamost_catalog_usage_example.py', 'w') as f:
        f.write(example_code)
    
    logger.info("Created lamost_catalog_usage_example.py with usage examples")

def main():
    parser = argparse.ArgumentParser(
        description="Download LAMOST catalog files and get spectrum access information"
    )
    parser.add_argument(
        "output_dir",
        type=str,
        help="Output directory for downloaded data"
    )
    parser.add_argument(
        "--dr_version",
        type=str,
        default="dr9_v2",
        choices=list(CATALOG_FILES.keys()),
        help="LAMOST data release version"
    )
    parser.add_argument(
        "--format",
        type=str,
        choices=['fits', 'csv'],
        default='fits',
        help="File format for catalogs"
    )
    parser.add_argument(
        "--analyze",
        action="store_true",
        help="Analyze downloaded catalogs"
    )
    parser.add_argument(
        "--examples",
        action="store_true",
        help="Generate example usage code"
    )
    
    args = parser.parse_args()
    
    # Create output directory
    output_path = Path(args.output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Starting LAMOST data download for {args.dr_version}")
    logger.info(f"Output directory: {args.output_dir}")
    
    # Download catalog files
    logger.info("Downloading catalog files...")
    catalog_files = download_catalog_files(args.dr_version, args.output_dir, args.format)
    
    if not catalog_files:
        logger.error("No catalog files were successfully downloaded")
        return
    
    logger.info(f"Successfully downloaded {len(catalog_files)} catalog files")
    
    # Analyze catalogs if requested
    if args.analyze:
        logger.info("Analyzing catalog files...")
        for catalog_file in catalog_files:
            analyze_catalog(catalog_file)
    
    # Generate examples if requested
    if args.examples:
        generate_example_usage()
    
    # Print spectrum access information
    print_spectrum_access_info()
    
    logger.info("Download complete!")
    logger.info(f"Catalog files saved to: {output_path / 'catalogs'}")

if __name__ == "__main__":
    main()
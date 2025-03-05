"""
Module for downloading and parsing TESS target lists from MIT's website.
This allows for more efficient downloading by pre-filtering targets.
"""

import os
import re
import logging
import requests
import time
from typing import List, Dict, Optional, Tuple, Set
import pandas as pd
from urllib.parse import urljoin
import numpy as np
import astropy.io.fits as fits

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('tess_target_lists')

# Base URL for TESS target lists
BASE_URL = "https://tess.mit.edu/public/target_lists/"

class TargetListManager:
    """
    Class to download and manage TESS target lists from MIT's website.
    
    This allows downloaders to pre-filter targets based on official target lists
    rather than downloading everything and filtering afterward.
    """
    
    def __init__(self, cache_dir: str = None):
        """
        Initialize the target list manager.
        
        Args:
            cache_dir: Directory to cache downloaded target lists
        """
        self.cache_dir = cache_dir or os.path.join(os.path.dirname(__file__), "target_lists_cache")
        os.makedirs(self.cache_dir, exist_ok=True)
        self.target_lists = {}  # Cache of loaded target lists
    
    def get_target_list_url(self, sector: int, cadence: str = "2m", gi_only: bool = False) -> str:
        """
        Construct the URL for a target list based on known patterns.
        
        Args:
            sector: TESS sector number
            cadence: "2m" or "20s"
            gi_only: If True, get only Guest Investigator targets
            
        Returns:
            URL for the target list CSV
        """
        # Format sector with leading zeros
        sector_str = f"S{sector:03d}"
        
        # Determine target type
        target_type = "GI_targets" if gi_only else "all_targets"
        
        # Determine cadence folder
        cadence_folder = cadence  # "2m" or "20s"
        
        # For 20s cadence, the filename includes "_20s"
        cadence_suffix = "_20s" if cadence == "20s" else ""
        
        # Construct URL
        url = f"{BASE_URL}{cadence_folder}/{target_type}{cadence_suffix}_{sector_str}_v1.csv"
        
        # Log the constructed URL for debugging
        logger.info(f"Constructed target list URL: {url}")
        
        return url
    
    def download_target_list(self, sector: int, cadence: str = "2m", gi_only: bool = False) -> str:
        """
        Download a target list file and return the path to the cached file.
        
        Args:
            sector: TESS sector number
            cadence: "2m" or "20s"
            gi_only: If True, get only Guest Investigator targets
            
        Returns:
            Path to the downloaded file
        """
        # Format sector with leading zeros
        sector_str = f"S{sector:03d}"
        
        # Determine target type
        target_type = "GI_targets" if gi_only else "all_targets"
        
        # Determine cadence folder
        cadence_folder = cadence  # "2m" or "20s"
        
        # For 20s cadence, the filename includes "_20s"
        cadence_suffix = "_20s" if cadence == "20s" else ""
        
        # Create cache filename
        cache_filename = f"{target_type}{cadence_suffix}_{sector_str}_v1.csv"
        cache_path = os.path.join(self.cache_dir, cache_filename)
        
        # If file already exists in cache, return it
        if os.path.exists(cache_path):
            logger.info(f"Using cached target list: {cache_path}")
            return cache_path
        
        # Try CSV first, then TXT if that fails
        for ext in ['csv', 'txt']:
            # Construct URL
            url = f"{BASE_URL}{cadence_folder}/{target_type}{cadence_suffix}_{sector_str}_v1.{ext}"
            
            # Download the file
            try:
                logger.info(f"Downloading target list from {url}")
                response = requests.get(url, timeout=30)
                response.raise_for_status()
                
                # Check if the response is actually a CSV/TXT file
                content_type = response.headers.get('Content-Type', '')
                if 'text/html' in content_type and '<html' in response.text[:100].lower():
                    logger.warning(f"Received HTML instead of data file from {url}")
                    continue  # Try next extension
                
                with open(cache_path, 'wb') as f:
                    f.write(response.content)
                    
                logger.info(f"Downloaded target list to {cache_path}")
                return cache_path
                
            except Exception as e:
                logger.warning(f"Failed to download target list from {url}: {e}")
                continue  # Try next extension
        
        # If we get here, all download attempts failed
        logger.error(f"Failed to download target list for Sector {sector} ({cadence})")
        
        # Create an empty placeholder file with a header row
        with open(cache_path, 'w') as f:
            f.write("TIC ID\n")  # Create a valid CSV with just a header
        
        logger.info(f"Created empty placeholder file: {cache_path}")
        return cache_path
    
    def get_target_ids(self, sector: int, cadence: str = "2m", gi_only: bool = False) -> Set[int]:
        """
        Get the set of target IDs for a specific sector and cadence
        
        Parameters
        ----------
        sector : int
            TESS sector number
        cadence : str
            Cadence type ("2m" or "20s")
        gi_only : bool
            If True, only return Guest Investigator targets
        
        Returns
        -------
        set
            Set of target IDs (integers)
        """
        # Download the target list if needed
        csv_path = self.download_target_list(sector, cadence, gi_only)
        
        # Try to read the CSV file directly with pandas
        try:
            # Skip comment lines with # prefix
            df = pd.read_csv(csv_path, comment='#')
            
            # Get the first column which should contain TIC IDs
            if len(df.columns) > 0:
                first_col = df.columns[0]
                tic_ids = set(df[first_col].astype(int))
                logger.info(f"Extracted {len(tic_ids)} TIC IDs from column '{first_col}'")
                logger.info(f"Sample TIC IDs: {list(tic_ids)[:5]}")
                return tic_ids
        except Exception as e:
            logger.warning(f"Error reading CSV with pandas: {e}")
        
        # If pandas fails, use direct parsing
        return self._parse_csv_directly(csv_path)
    
    def _parse_csv_directly(self, csv_path: str) -> Set[int]:
        """
        Parse the CSV file directly to extract TIC IDs.
        
        Args:
            csv_path: Path to the CSV file
            
        Returns:
            Set of target IDs (TIC IDs)
        """
        target_ids = set()
        header_found = False
        
        with open(csv_path, 'r') as f:
            for line in f:
                line = line.strip()
                
                # Skip comment lines and empty lines
                if not line or line.startswith('#'):
                    continue
                
                # Skip the header line
                if not header_found and ('TICID' in line or 'TIC' in line):
                    header_found = True
                    continue
                
                # Parse the CSV line
                parts = line.split(',')
                if not parts:
                    continue
                
                # First field should be the TIC ID
                try:
                    tic_id = int(parts[0])
                    target_ids.add(tic_id)
                except (ValueError, IndexError):
                    # If first field fails, try to find a numeric field
                    for part in parts:
                        try:
                            tic_id = int(part.strip())
                            target_ids.add(tic_id)
                            break
                        except ValueError:
                            continue
        
        logger.info(f"Extracted {len(target_ids)} TIC IDs using direct parsing")
        if target_ids:
            logger.info(f"Sample TIC IDs: {list(target_ids)[:5]}")
            return target_ids
        
        logger.warning(f"Direct parsing found no TIC IDs")
        return set()

def verify_cadence(fits_file, expected_cadence):
    """Verify that the time step in the FITS file matches the expected cadence"""
    with fits.open(fits_file) as hdul:
        # Get the time array
        time = hdul[1].data['TIME']
        
        # Calculate median time step in seconds
        time_diffs = np.diff(time) * 24 * 3600  # Convert from days to seconds
        median_step = np.median(time_diffs)
        
        # Check if median step matches expected cadence
        if expected_cadence == "2m":
            expected_step = 120  # 2 minutes in seconds
        elif expected_cadence == "20s":
            expected_step = 20   # 20 seconds in seconds
        else:
            return False
        
        # Allow for some tolerance (e.g., 10%)
        tolerance = 0.1 * expected_step
        
        return abs(median_step - expected_step) <= tolerance 
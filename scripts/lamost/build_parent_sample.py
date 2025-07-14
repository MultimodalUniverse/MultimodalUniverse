import argparse
import os
import urllib.request
import gzip
import shutil
from functools import partial
from multiprocessing import Pool

import h5py
import healpy as hp
import numpy as np
from astropy.io import fits
from astropy.table import Table, hstack
from tqdm import tqdm
from tqdm.contrib.concurrent import process_map

_healpix_nside = 16

# LAMOST wavelength coverage: 3700-9000 Å
_wavelength_start = 3700.0
_wavelength_end = 9000.0
_wavelength_step = 1.0

def get_wavelength_grid():
    """Generate standard LAMOST wavelength grid"""
    return np.arange(_wavelength_start, _wavelength_end + _wavelength_step, _wavelength_step, dtype=np.float32)

def download_lamost_catalog(output_dir, dr_version="dr9"):
    """Download LAMOST catalog file"""
    catalog_dir = os.path.join(output_dir, "catalogs")
    os.makedirs(catalog_dir, exist_ok=True)
    
    # Example URL - adjust based on actual LAMOST data release structure
    catalog_filename = f"lamost_{dr_version}_catalog.fits"
    catalog_path = os.path.join(catalog_dir, catalog_filename)
    
    if not os.path.exists(catalog_path):
        print(f"Downloading LAMOST {dr_version} catalog...")
        # Note: Replace with actual LAMOST catalog URL
        catalog_url = f"http://dr9.lamost.org/catalog/{catalog_filename}"
        try:
            urllib.request.urlretrieve(catalog_url, catalog_path)
            print(f"Catalog downloaded to {catalog_path}")
        except Exception as e:
            print(f"Error downloading catalog: {e}")
            print("Please manually download the LAMOST catalog and place it in the catalogs directory")
            return None
    
    return catalog_path

def download_lamost_spectrum(spectrum_info, base_path):
    """Download individual LAMOST spectrum file"""
    obsid, designation, filename = spectrum_info
    
    # Create directory structure based on observation ID
    spectrum_dir = os.path.join(base_path, "spectra", obsid[:4], obsid[4:6])
    os.makedirs(spectrum_dir, exist_ok=True)
    
    spectrum_path = os.path.join(spectrum_dir, filename)
    
    if os.path.exists(spectrum_path):
        return spectrum_path
    
    # Construct download URL based on LAMOST data structure
    spectrum_url = f"http://dr9.lamost.org/spectra/{obsid[:4]}/{obsid[4:6]}/{filename}"
    
    try:
        urllib.request.urlretrieve(spectrum_url, spectrum_path)
        
        # Handle gzipped files
        if filename.endswith('.gz'):
            with gzip.open(spectrum_path, 'rb') as f_in:
                with open(spectrum_path[:-3], 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            os.remove(spectrum_path)
            spectrum_path = spectrum_path[:-3]
        
        return spectrum_path
    except Exception as e:
        print(f"Error downloading spectrum {filename}: {e}")
        return None

def check_spectrum_exists(spectrum_info, base_path):
    """Check if spectrum file exists locally"""
    obsid, designation, filename = spectrum_info
    spectrum_dir = os.path.join(base_path, "spectra", obsid[:4], obsid[4:6])
    spectrum_path = os.path.join(spectrum_dir, filename)
    
    # Check both .fits and .fits.gz versions
    if os.path.exists(spectrum_path):
        return True
    if os.path.exists(spectrum_path + '.gz'):
        return True
    if filename.endswith('.gz') and os.path.exists(spectrum_path[:-3]):
        return True
    
    return False

def selection_fn(catalog, base_path, check_exists=True):
    """Apply selection cuts to the LAMOST catalog"""
    # Basic quality cuts
    mask = catalog["snr"] > 20  # SNR > 20
    mask &= catalog["mag_r"] < 19.0  # r magnitude < 19
    mask &= catalog["class"] != "UNKNOWN"  # Exclude unknown objects
    mask &= catalog["rv_flag"] == 0  # Reliable radial velocity
    
    # Remove entries with missing essential data
    mask &= ~np.isnan(catalog["ra"])
    mask &= ~np.isnan(catalog["dec"])
    mask &= ~np.isnan(catalog["snr"])
    
    # Remove duplicates based on designation
    _, idx = np.unique(catalog["designation"], return_index=True)
    unique_mask = np.zeros(len(catalog), dtype=bool)
    unique_mask[idx] = True
    mask &= unique_mask
    
    if check_exists:
        # Check if spectrum files exist
        spectrum_info = list(zip(catalog["obsid"], catalog["designation"], catalog["filename"]))
        exists_mask = process_map(
            partial(check_spectrum_exists, base_path=base_path),
            spectrum_info,
            desc="Checking spectrum files",
            chunksize=100
        )
        mask &= np.array(exists_mask)
    
    return mask

def process_lamost_spectrum(spectrum_path):
    """Process a single LAMOST spectrum file"""
    try:
        with fits.open(spectrum_path) as hdul:
            # LAMOST spectrum structure (adjust based on actual format)
            flux = hdul[0].data.astype(np.float32)
            
            # Get wavelength information from header
            header = hdul[0].header
            crval1 = header.get('CRVAL1', _wavelength_start)
            cdelt1 = header.get('CDELT1', _wavelength_step)
            crpix1 = header.get('CRPIX1', 1)
            
            # Generate wavelength array
            npoints = len(flux)
            wavelength = (crval1 + (np.arange(npoints) - crpix1 + 1) * cdelt1).astype(np.float32)
            
            # Estimate inverse variance (adjust based on actual error extension)
            if len(hdul) > 1 and hdul[1].data is not None:
                error = hdul[1].data.astype(np.float32)
                ivar = np.where(error > 0, 1.0 / error**2, 0.0)
            else:
                # Simple estimate if no error array
                ivar = np.where(flux > 0, 1.0 / np.abs(flux), 0.0)
            
            # Create mask for bad pixels
            mask = np.isnan(flux) | np.isnan(ivar) | (flux <= 0) | (ivar <= 0)
            
            # Simple continuum normalization
            # Use a running median for continuum estimation
            from scipy.ndimage import median_filter
            continuum = median_filter(flux, size=51)
            continuum = np.where(continuum > 0, continuum, 1.0)
            norm_flux = flux / continuum
            norm_ivar = ivar * continuum**2
            
            return {
                "spectrum_lambda": wavelength,
                "spectrum_flux": flux,
                "spectrum_ivar": ivar,
                "spectrum_mask": mask,
                "spectrum_norm_flux": norm_flux,
                "spectrum_norm_ivar": norm_ivar,
            }
    except Exception as e:
        print(f"Error processing spectrum {spectrum_path}: {e}")
        return None

def save_in_standard_format(args):
    """Save processed spectra in standard HDF5 format"""
    catalog_group, output_filename, base_path = args
    
    # Create output directory if it doesn't exist
    os.makedirs(os.path.dirname(output_filename), exist_ok=True)
    
    try:
        # Download spectra if needed
        spectrum_results = []
        for row in catalog_group:
            spectrum_info = (row["obsid"], row["designation"], row["filename"])
            spectrum_path = download_lamost_spectrum(spectrum_info, base_path)
            
            if spectrum_path:
                spectrum_data = process_lamost_spectrum(spectrum_path)
                if spectrum_data:
                    spectrum_results.append(spectrum_data)
                else:
                    print(f"Failed to process spectrum for {row['designation']}")
            else:
                print(f"Failed to download spectrum for {row['designation']}")
        
        if not spectrum_results:
            print(f"No valid spectra found for {output_filename}")
            return 0
        
        # Stack all spectrum arrays
        spectra_table = Table({
            k: np.vstack([result[k] for result in spectrum_results])
            for k in spectrum_results[0].keys()
        })
        
        # Add standard columns
        catalog_group["object_id"] = catalog_group["designation"]
        catalog_group["restframe"] = np.ones(len(catalog_group), dtype=bool)
        
        # Combine catalog and spectra data
        final_table = hstack([catalog_group, spectra_table])
        
        # Save to HDF5
        with h5py.File(output_filename, "w") as hdf5_file:
            for key in final_table.colnames:
                hdf5_file.create_dataset(key.lower(), data=final_table[key])
        
        print(f"Saved {len(final_table)} objects to {output_filename}")
        return 1
        
    except Exception as e:
        print(f"Error processing {output_filename}: {e}")
        return 0

def main(args):
    """Main processing function"""
    # Download catalog if needed
    catalog_path = download_lamost_catalog(args.lamost_data_path, args.dr_version)
    if not catalog_path:
        print("Failed to obtain catalog file")
        return
    
    # Load catalog
    print(f"Loading catalog from {catalog_path}")
    try:
        catalog = Table.read(catalog_path, hdu=1)
    except Exception as e:
        print(f"Error reading catalog: {e}")
        print("Please check the catalog file format and path")
        return
    
    # Apply tiny subset if requested
    if args.tiny:
        catalog = catalog[:100]
        print(f"Using tiny subset of {len(catalog)} objects")
    
    # Apply selection cuts
    print("Applying selection cuts...")
    mask = selection_fn(catalog, args.lamost_data_path, not args.skip_check)
    catalog = catalog[mask]
    print(f"Selected {len(catalog)} objects after cuts")
    
    if len(catalog) == 0:
        print("No objects passed selection cuts")
        return
    
    # Calculate healpix indices
    print("Calculating healpix indices...")
    catalog["healpix"] = hp.ang2pix(
        _healpix_nside,
        catalog["ra"],
        catalog["dec"],
        lonlat=True,
        nest=True
    )
    
    # Group by healpix
    catalog_grouped = catalog.group_by("healpix")
    
    # Prepare arguments for parallel processing
    map_args = []
    for group in catalog_grouped.groups:
        output_filename = os.path.join(
            args.output_dir,
            f"lamost/healpix={group['healpix'][0]}/001-of-001.hdf5"
        )
        map_args.append((group, output_filename, args.lamost_data_path))
    
    print(f"Processing {len(map_args)} healpix groups...")
    
    # Process in parallel
    with Pool(args.num_processes) as pool:
        results = list(
            tqdm(pool.imap(save_in_standard_format, map_args), total=len(map_args))
        )
    
    successful = sum(results)
    print(f"Successfully processed {successful}/{len(map_args)} healpix groups")
    
    if successful < len(map_args):
        print("Some files failed to process. Check error messages above.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Download and process LAMOST spectra to create parent sample"
    )
    parser.add_argument(
        "lamost_data_path",
        type=str,
        help="Path to store/find LAMOST data"
    )
    parser.add_argument(
        "output_dir",
        type=str,
        help="Output directory for processed data"
    )
    parser.add_argument(
        "--dr_version",
        type=str,
        default="dr9",
        help="LAMOST data release version (default: dr9)"
    )
    parser.add_argument(
        "--num_processes",
        type=int,
        default=os.cpu_count(),
        help="Number of parallel processes"
    )
    parser.add_argument(
        "--tiny",
        action="store_true",
        help="Process only a small subset for testing"
    )
    parser.add_argument(
        "--skip_check",
        action="store_true",
        help="Skip checking if spectrum files exist"
    )
    
    args = parser.parse_args()
    main(args)
#!/usr/bin/env python3
"""
LAMOST Spectrum Downloader
Downloads LAMOST spectra files given observation IDs (obsid)
"""

import argparse
import gzip
import os
import shutil
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from multiprocessing import cpu_count
from pathlib import Path
from typing import Literal

import numpy as np
import requests
from astropy.table import Table


def extract_gz_file(gz_path):
    """
    Extract a .gz file and delete the original

    Args:
        gz_path (Path): Path to the .gz file

    Returns:
        Path: Path to the extracted file, or None if failed
    """
    try:
        # Determine output filename (remove .gz extension)
        output_path = gz_path.with_suffix("")

        print(f"Extracting {gz_path.name}...")

        # Extract the file
        with gzip.open(gz_path, "rb") as f_in:
            with open(output_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)

        # Delete the .gz file
        gz_path.unlink()

        extracted_size = output_path.stat().st_size
        print(f"Extracted to {output_path.name} ({extracted_size:,} bytes)")
        return output_path

    except Exception as e:
        print(f"Error extracting {gz_path.name}: {e}")
        return None


def download_spectrum(obsid, token, output_dir=".", timeout=30, retries=3):
    """
    Download a LAMOST spectrum file given an observation ID
    Automatically extracts .gz files and deletes the compressed version

    Args:
        obsid (str): Observation ID
        token (str): Authentication token
        output_dir (str): Directory to save the file
        timeout (int): Request timeout in seconds
        retries (int): Number of retry attempts

    Returns:
        bool: True if successful, False otherwise
    """
    base_url = "https://www.lamost.org/dr10/v2.0/spectrum/fits"
    url = f"{base_url}/{obsid}?token={token}"

    # Create output directory if it doesn't exist
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    for attempt in range(retries):
        try:
            print(f"Downloading obsid {obsid} (attempt {attempt + 1}/{retries})...")

            # Make the request
            response = requests.get(url, timeout=timeout, stream=True)
            response.raise_for_status()

            # Try to get filename from Content-Disposition header
            filename = None
            if "Content-Disposition" in response.headers:
                content_disp = response.headers["Content-Disposition"]
                if "filename=" in content_disp:
                    filename = content_disp.split("filename=")[1].strip('"')

            # Use obsid-based filename (override Content-Disposition for consistency)
            # Assume downloaded file is compressed
            filename = f"{obsid}.fits.gz"

            # Full path for output file
            output_path = Path(output_dir) / filename

            # Check if extracted file already exists (without .gz)
            if filename.endswith(".gz"):
                extracted_path = output_path.with_suffix("")
                if extracted_path.exists():
                    print(
                        f"Extracted file {extracted_path.name} already exists, skipping..."
                    )
                    return True
            elif output_path.exists():
                print(f"File {filename} already exists, skipping...")
                return True

            # Download and save the file
            with open(output_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

            file_size = output_path.stat().st_size
            print(f"Successfully downloaded {filename} ({file_size:,} bytes)")

            # Extract if it's a .gz file
            if filename.endswith(".gz"):
                extracted_path = extract_gz_file(output_path)
                if extracted_path is None:
                    return False

            return True

        except requests.exceptions.RequestException as e:
            print(f"Error downloading obsid {obsid}: {e}")
            if attempt < retries - 1:
                print("Retrying in 2 seconds...")
                time.sleep(2)
            else:
                print(f"Failed to download obsid {obsid} after {retries} attempts")
                return False
        except Exception as e:
            print(f"Unexpected error: {e}")
            return False

    return False


def download_spectrum_worker(args):
    """
    Worker function for multiprocessing download

    Args:
        args (tuple): (obsid, token, output_dir, timeout, retries)

    Returns:
        tuple: (obsid, success, error_message)
    """
    obsid, token, output_dir, timeout, retries = args

    try:
        success = download_spectrum(obsid, token, output_dir, timeout, retries)
        return (obsid, success, None)
    except Exception as e:
        return (obsid, False, str(e))


def download_from_catalog(
    catalog_path,
    token="F2f59e87b65",
    output_dir=".",
    max_iterations=np.inf,
    n_workers=None,
    timeout=30,
    retries=3,
    delay=0.1,
):
    """
    Download LAMOST spectra from a catalog file using multiprocessing

    Args:
        catalog_path (str): Path to LAMOST catalog FITS file
        token (str): Authentication token. You can get it when downloading urls file from LAMOST website.
        output_dir (str): Directory to save files
        max_iterations (int): Maximum number of spectra to download (default: np.inf)
        n_workers (int): Number of parallel workers (default: cpu_count())
        timeout (int): Request timeout in seconds
        retries (int): Number of retry attempts
        delay (float): Delay between batch starts in seconds

    Returns:
        dict: Summary of download results
    """
    print(f"Reading catalog from {catalog_path}...")

    os.makedirs(output_dir, exist_ok=True)

    try:
        # Read the catalog
        catalog = Table.read(catalog_path)
        print(f"Catalog loaded with {len(catalog)} entries")

        # Find the obsid column (try common names)
        obsid_col = None
        possible_obsid_cols = ["obsid", "obsID", "ObsID"]

        for col in possible_obsid_cols:
            if col in catalog.colnames:
                obsid_col = col
                break

        if obsid_col is None:
            print("Available columns:", catalog.colnames)
            raise ValueError("Could not find obsid column. Please check column names.")

        print(f"Using '{obsid_col}' as obsid column")

        # Get obsids and limit by max_iterations
        obsids = catalog[obsid_col].data
        if max_iterations != np.inf:
            max_iterations = int(max_iterations)
            obsids = obsids[:max_iterations]

        print(f"Will process {len(obsids)} observations")

        # Create output directory
        Path(output_dir).mkdir(parents=True, exist_ok=True)

        # Filter out already existing files
        obsids_to_download = []
        existing_count = 0

        for obsid in obsids:
            output_path = Path(output_dir) / f"{obsid}.fits"
            if not output_path.exists():
                obsids_to_download.append(str(obsid))
            else:
                existing_count += 1

        print(
            f"Found {existing_count} existing files, will download {len(obsids_to_download)} new files"
        )

        if not obsids_to_download:
            print("All files already exist!")
            return {
                "total": len(obsids),
                "existing": existing_count,
                "downloaded": 0,
                "successful": 0,
                "failed": 0,
            }

        # Set up multiprocessing
        if n_workers is None:
            n_workers = min(
                cpu_count(), len(obsids_to_download), 128
            )  # Cap at 128 to be nice to servers

        print(f"Using {n_workers} parallel workers")

        # Prepare arguments for workers
        worker_args = [
            (obsid, token, output_dir, timeout, retries) for obsid in obsids_to_download
        ]

        # Track progress
        successful = 0
        failed = 0
        failed_obsids = []

        # Process in batches to avoid overwhelming the server
        batch_size = n_workers * 2
        total_batches = (len(worker_args) + batch_size - 1) // batch_size

        print(f"Processing in {total_batches} batches of {batch_size} each")

        for batch_idx in range(total_batches):
            start_idx = batch_idx * batch_size
            end_idx = min((batch_idx + 1) * batch_size, len(worker_args))
            batch_args = worker_args[start_idx:end_idx]

            print(f"\nBatch {batch_idx + 1}/{total_batches} ({len(batch_args)} files)")

            # Use ThreadPoolExecutor for I/O bound tasks (better than ProcessPoolExecutor for network)
            with ThreadPoolExecutor(max_workers=n_workers) as executor:
                # Submit all tasks
                future_to_obsid = {
                    executor.submit(download_spectrum_worker, args): args[0]
                    for args in batch_args
                }

                # Process completed tasks
                for future in as_completed(future_to_obsid):
                    obsid, success, error = future.result()

                    if success:
                        successful += 1
                    else:
                        failed += 1
                        failed_obsids.append((obsid, error))

            # Progress report
            total_processed = successful + failed
            progress = (batch_idx + 1) / total_batches * 100
            print(
                f"Batch complete. Progress: {total_processed}/{len(obsids_to_download)} "
                f"({progress:.1f}%) - Success: {successful}, Failed: {failed}"
            )

            # Small delay between batches
            if batch_idx < total_batches - 1 and delay > 0:
                time.sleep(delay)

        # Final summary
        print("\n" + "=" * 50)
        print("DOWNLOAD COMPLETE")
        print("=" * 50)
        print(f"Total obsids in catalog: {len(obsids)}")
        print(f"Already existing: {existing_count}")
        print(f"Attempted downloads: {len(obsids_to_download)}")
        print(f"Successful downloads: {successful}")
        print(f"Failed downloads: {failed}")
        print(f"Success rate: {successful / len(obsids_to_download) * 100:.1f}%")

        if failed_obsids:
            print("\nFailed obsids:")
            for obsid, error in failed_obsids[:10]:  # Show first 10 failures
                print(f"  {obsid}: {error}")
            if len(failed_obsids) > 10:
                print(f"  ... and {len(failed_obsids) - 10} more")

        return {
            "total": len(obsids),
            "existing": existing_count,
            "attempted": len(obsids_to_download),
            "successful": successful,
            "failed": failed,
            "failed_obsids": failed_obsids,
        }

    except Exception as e:
        print(f"Error processing catalog: {e}")
        return None
    """
    Download multiple spectra with optional delay between downloads
    Automatically extracts .gz files and deletes compressed versions
    
    Args:
        obsid_list (list): List of observation IDs
        token (str): Authentication token
        output_dir (str): Directory to save files
        delay (float): Delay between downloads in seconds
    """
    successful = 0
    failed = 0

    for i, obsid in enumerate(obsid_list):
        print(f"\nProgress: {i + 1}/{len(obsid_list)}")

        if download_spectrum(obsid, token, output_dir):
            successful += 1
        else:
            failed += 1

        # Add delay between downloads (except for the last one)
        if i < len(obsid_list) - 1 and delay > 0:
            time.sleep(delay)

    print("\nDownload complete!")
    print(f"Successful: {successful}")
    print(f"Failed: {failed}")
    print(f"Total: {len(obsid_list)}")


def download_catalog(
    catalog_name: Literal[
        "LRS_catalogue",
        "LRS_stellar",
        "LRS_qso",
        "LRS_galaxy",
        "LRS_wd",
        "LRS_astellar",
        "LRS_mstellar",
        "LRS_cv",
        "MRS_catalogue",
        "MRS_stellar",
    ],
    release: str = "dr10_v2.0",
):
    _release = release.replace("_", "/")
    if catalog_name.startswith("MRS"):
        _release = release + "/medcas"
    # https://www.lamost.org/dr10/v2.0/catdl?name=dr10_v2.0_LRS_catalogue.fits.gz
    url = (
        f"https://www.lamost.org/{_release}/catdl?name={release}_{catalog_name}.fits.gz"
    )
    # Get filename from URL
    filename = url.split("name=")[1]
    extracted_filename = filename.replace(".gz", "")

    # Check if extracted file already exists
    if os.path.exists(extracted_filename):
        print(f"Catalog {extracted_filename} already exists, skipping download...")
        return Path(extracted_filename)

    print(f"Downloading catalog {catalog_name} from {url}...")
    try:
        response = requests.get(url, timeout=30, stream=True)
        response.raise_for_status()

        # Get total file size from Content-Length header
        total_size = int(response.headers.get("content-length", 0))

        downloaded = 0
        with open(filename, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)

                    # Display progress
                    if total_size > 0:
                        progress = (downloaded / total_size) * 100
                        print(
                            f"\rProgress: {progress:.1f}% ({downloaded:,}/{total_size:,} bytes)",
                            end="",
                        )
                    else:
                        print(f"\rDownloaded: {downloaded:,} bytes", end="")

        print()  # New line after progress

        file_size = os.path.getsize(filename)
        print(f"Successfully downloaded {filename} ({file_size:,} bytes)")

        # Extract the .gz file
        extracted_path = extract_gz_file(Path(filename))
        if extracted_path is None:
            print("Failed to extract catalog")
            return None

        return extracted_path

    except requests.exceptions.RequestException as e:
        print(f"Error downloading catalog: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error: {e}")
        return None


def main():
    parser = argparse.ArgumentParser(
        description="Download LAMOST DR10 spectrum files and extract .gz files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
        Example usage:
        python download_data.py lamost_catalog.fits -t F2f59e87b65 -o ./lamost_spectra -i 1000 -n 8 -d 1.0 --timeout 30 --retries 3
        This will download up to 1000 spectra from the catalog file 'lamost_catalog.fits',
        using 8 parallel workers, with a 1 second delay between downloads.
        The output files will be saved in the './lamost_spectra' directory.
        The authentication token is set to 'F2f59e87b65' (default value).
        You can adjust the number of workers, delay, timeout, and retries as needed.
        """,
    )

    parser.add_argument(
        "--catalog_name",
        help="Name of LAMOST catalog to download",
        choices=[
            "LRS_catalogue",
            "LRS_stellar",
            "LRS_qso",
            "LRS_galaxy",
            "LRS_wd",
            "LRS_astellar",
            "LRS_mstellar",
            "LRS_cv",
            "MRS_catalogue",
            "MRS_stellar",
        ],
        required=True,
    )
    parser.add_argument(
        "--release",
        default="dr10_v2.0",
        help="Data release version (default: dr10_v2.0)",
        required=True,
    )
    # parser.add_argument("catalog_path", help="Path to LAMOST catalog FITS file")
    parser.add_argument(
        "-o",
        "--output",
        default=".",
        help="Output directory (default: current directory)",
    )
    parser.add_argument(
        "-i",
        "--max_rows",
        type=int,
        default=1000,
        help="Maximum number of spectra to download (default: 1000)",
    )
    parser.add_argument(
        "-n",
        "--n_workers",
        type=int,
        default=None,
        help="Number of parallel workers (default: auto-detect)",
    )
    parser.add_argument(
        "-t", "--token", default="F2f59e87b65", help="Authentication token"
    )
    parser.add_argument(
        "-d",
        "--delay",
        type=float,
        default=1.0,
        help="Delay between downloads in seconds (default: 1.0)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="Request timeout in seconds (default: 30)",
    )
    parser.add_argument(
        "--retries", type=int, default=3, help="Number of retry attempts (default: 3)"
    )

    args = parser.parse_args()

    catalog_path = download_catalog(args.catalog_name, args.release)
    if catalog_path is None:
        print("Failed to download catalog, exiting.")
        return

    download_from_catalog(
        catalog_path=catalog_path,
        token=args.token,
        output_dir=args.output,
        max_iterations=args.max_rows,
        n_workers=args.n_workers,
        timeout=args.timeout,
        retries=args.retries,
        delay=args.delay,
    )


if __name__ == "__main__":
    main()

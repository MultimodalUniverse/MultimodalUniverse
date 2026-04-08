#!/usr/bin/env python3
"""
LAMOST Spectrum Downloader

Downloads LAMOST spectra as date-level tar.gz archives without extracting them.
Each tar.gz contains all spectra for one observation night (with individual
.fits.gz files inside).

Workflow:
  1. Downloads the LAMOST catalog (.fits.gz) and extracts it.
  2. Reads unique observation dates from the catalog.
  3. Downloads one tar.gz per date into the output directory.
     Already-downloaded tar.gz files are skipped (resume-safe).

The output directory will contain files like:
  <output_dir>/20111024.tar.gz
  <output_dir>/20111025.tar.gz
  ...

These tar.gz files are consumed by build_parent_sample.py, which reads
spectra directly from them and produces healpix-grouped HDF5 files.
"""

import argparse
import gzip
import os
import re
import shutil
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Literal

import numpy as np
import requests
from astropy.table import Table
from tqdm import tqdm


TAR_INDEX_URLS = {
    "LRS": "https://www.lamost.org/{release}/tar/lrs-fits/",
    "MRS": "https://www.lamost.org/{release}/tar/mrs-fits/",
}


def extract_gz_file(gz_path):
    """Extract a .gz file and delete the original."""
    try:
        output_path = gz_path.with_suffix("")
        with gzip.open(gz_path, "rb") as f_in:
            with open(output_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
        gz_path.unlink()
        return output_path
    except Exception as e:
        print(f"Error extracting {gz_path.name}: {e}")
        return None


def get_available_dates(survey_type, release="dr11_v2.0"):
    """
    Scrape the LAMOST tar index page to get all available observation dates.

    Returns:
        list[str]: Sorted list of date strings like ['20111024', '20111025', ...]
    """
    _release = release.replace("_", "/")
    url = TAR_INDEX_URLS[survey_type].format(release=_release)
    print(f"Fetching available dates from {url}...")

    response = requests.get(url, timeout=60)
    response.raise_for_status()

    dates = re.findall(r'(\d{8})\.tar\.gz', response.text)
    dates = sorted(set(dates))
    print(f"Found {len(dates)} available observation dates")
    return dates


def get_dates_from_catalog(catalog_path, max_rows=None, rank=0, world_size=1):
    """
    Read the catalog and extract the unique observation dates.

    Returns:
        list[str]: Sorted list of unique obsdate strings like ['20111024', ...]
    """
    print(f"Reading catalog from {catalog_path}...")
    catalog = Table.read(catalog_path)
    print(f"Catalog loaded with {len(catalog)} entries")

    if "obsdate" not in catalog.colnames:
        raise ValueError(f"Could not find 'obsdate' column. Available: {catalog.colnames}")

    if max_rows is not None:
        catalog = catalog[:max_rows]

    dates = sorted(set(d.replace("-", "") for d in catalog["obsdate"]))
    print(f"Catalog spans {len(dates)} unique observation dates")

    if world_size > 1:
        total = len(dates)
        dates = dates[rank::world_size]
        print(f"[rank {rank}/{world_size}] Shard: {len(dates)} of {total} dates")

    return dates


def download_tars(
    catalog_path,
    survey_type="LRS",
    output_dir=".",
    max_rows=None,
    release="dr11_v2.0",
    rank=0,
    world_size=1,
    use_aria2=True,
):
    """
    Download LAMOST spectra as date-level tar.gz archives (without extracting).

    1. Reads the catalog to determine which observation dates are needed.
    2. Scrapes the index page for available tar.gz files.
    3. Downloads missing tar.gz files (with aria2c or requests).
    """
    os.makedirs(output_dir, exist_ok=True)

    catalog_dates = get_dates_from_catalog(
        catalog_path, max_rows=max_rows, rank=rank, world_size=world_size
    )

    available_dates = get_available_dates(survey_type, release)
    available_set = set(available_dates)

    dates_to_download = [d for d in catalog_dates if d in available_set]
    missing = [d for d in catalog_dates if d not in available_set]
    if missing:
        print(f"WARNING: {len(missing)} catalog dates not found on server: {missing[:10]}{'...' if len(missing) > 10 else ''}")

    already_downloaded = []
    needed = []
    for d in dates_to_download:
        tar_path = Path(output_dir) / f"{d}.tar.gz"
        if tar_path.exists():
            already_downloaded.append(d)
        else:
            needed.append(d)

    print(f"Dates: {len(dates_to_download)} total, {len(already_downloaded)} already downloaded, {len(needed)} to download")

    if not needed:
        print("All tar.gz files already downloaded!")
        return

    _release = release.replace("_", "/")
    base_url = TAR_INDEX_URLS[survey_type].format(release=_release)

    if use_aria2:
        _download_tars_aria2(needed, base_url, output_dir)
    else:
        _download_tars_requests(needed, base_url, output_dir)

    downloaded_count = sum(1 for d in needed if (Path(output_dir) / f"{d}.tar.gz").exists())

    print("\n" + "=" * 50)
    print("DOWNLOAD COMPLETE")
    print("=" * 50)
    print(f"Total dates in catalog: {len(catalog_dates)}")
    print(f"Dates available on server: {len(dates_to_download)}")
    print(f"Previously downloaded: {len(already_downloaded)}")
    print(f"Newly downloaded: {downloaded_count}")
    if downloaded_count < len(needed):
        print(f"Failed: {len(needed) - downloaded_count}")


def _download_tars_aria2(dates, base_url, output_dir):
    """Download tar.gz files using aria2c."""
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".txt", delete=False, prefix="aria2_tars_"
    ) as f:
        url_file = f.name
        for d in dates:
            url = f"{base_url}{d}.tar.gz"
            f.write(f"{url}\n  out={d}.tar.gz\n")

    print(f"Downloading {len(dates)} tar.gz files with aria2c...")
    try:
        subprocess.run(
            [
                "aria2c",
                "-j4",
                "-s16",
                "-x16",
                "-c",
                "--dir", str(output_dir),
                "--input-file", url_file,
                "--auto-file-renaming=false",
                "--allow-overwrite=false",
            ],
            check=False,
        )
    finally:
        os.unlink(url_file)


def _download_tars_requests(dates, base_url, output_dir, retries=3):
    """Download tar.gz files using requests with retries."""
    for d in tqdm(dates, desc="Downloading tar.gz files"):
        url = f"{base_url}{d}.tar.gz"
        output_path = Path(output_dir) / f"{d}.tar.gz"

        for attempt in range(retries):
            try:
                response = requests.get(url, timeout=300, stream=True)
                response.raise_for_status()

                total_size = int(response.headers.get("content-length", 0))
                with open(output_path, "wb") as f:
                    downloaded = 0
                    for chunk in response.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            f.write(chunk)
                            downloaded += len(chunk)

                print(f"Downloaded {d}.tar.gz ({output_path.stat().st_size:,} bytes)")
                break

            except Exception as e:
                print(f"Error downloading {d}.tar.gz (attempt {attempt + 1}/{retries}): {e}")
                if attempt < retries - 1:
                    time.sleep(2)
                else:
                    print(f"Failed to download {d}.tar.gz after {retries} attempts")


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
    release: str = "dr11_v2.0",
    use_aria2: bool = False,
):
    _release = release.replace("_", "/")
    if catalog_name.startswith("MRS"):
        _release = _release + "/medcas"
    url = (
        f"https://www.lamost.org/{_release}/catdl?name={release}_{catalog_name}.fits.gz"
    )
    filename = url.split("name=")[1]
    extracted_filename = filename.replace(".gz", "")

    if os.path.exists(extracted_filename):
        print(f"Catalog {extracted_filename} already exists, skipping download...")
        return Path(extracted_filename)

    if os.path.exists(filename):
        print(f"Compressed catalog {filename} already exists, extracting...")
        extracted_path = extract_gz_file(Path(filename))
        if extracted_path is not None:
            return extracted_path
        else:
            print("Failed to extract existing compressed file, re-downloading...")

    print(f"Downloading catalog {catalog_name} from {url}...")
    try:
        if use_aria2:
            result = subprocess.run(
                [
                    "aria2c",
                    "-x16",
                    "-s16",
                    "--out", filename,
                    url,
                ],
                check=False,
            )
            if result.returncode != 0:
                print(f"aria2c exited with code {result.returncode}")
                return None
        else:
            response = requests.get(url, timeout=30, stream=True)
            response.raise_for_status()

            total_size = int(response.headers.get("content-length", 0))

            downloaded = 0
            with open(filename, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)

                        if total_size > 0:
                            progress = (downloaded / total_size) * 100
                            print(
                                f"\rProgress: {progress:.1f}% ({downloaded:,}/{total_size:,} bytes)",
                                end="",
                            )
                        else:
                            print(f"\rDownloaded: {downloaded:,} bytes", end="")

            print()

        file_size = os.path.getsize(filename)
        print(f"Successfully downloaded {filename} ({file_size:,} bytes)")

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
        description="Download LAMOST spectra as date-level tar.gz archives",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Example usage:
  python download_data.py --catalog_name LRS_stellar --release dr11_v2.0 -o ./lamost_spectra
  python download_data.py --catalog_name MRS_stellar --release dr11_v2.0 -o ./lamost_mrs_spectra

Multi-node usage (e.g. 4 nodes writing to a shared filesystem):
  python download_data.py --catalog_name LRS_stellar -o /shared/lamost --rank 0 --world_size 4
  python download_data.py --catalog_name LRS_stellar -o /shared/lamost --rank 1 --world_size 4

The catalog is downloaded automatically from the LAMOST website. Unique
observation dates are extracted from the catalog, and the corresponding
tar.gz archives (one per night) are downloaded into the output directory.
The tar.gz files are NOT extracted; they are consumed directly by
build_parent_sample.py.
        """,
    )

    catalog_choices = [
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
    ]

    parser.add_argument(
        "--catalog_name",
        help="Name of LAMOST catalog to download",
        choices=catalog_choices,
        required=True,
    )
    parser.add_argument(
        "--release",
        default="dr11_v2.0",
        help="Data release version (default: dr11_v2.0)",
    )
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
        default=None,
        help="Use only the first N rows of the catalog to determine dates (default: all)",
    )
    parser.add_argument(
        "--use_aria2",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Use aria2c for faster downloads (default: True). Use --no-use_aria2 to disable.",
    )
    parser.add_argument(
        "--rank",
        type=int,
        default=0,
        help="Node rank for multi-node downloading (default: 0)",
    )
    parser.add_argument(
        "--world_size",
        type=int,
        default=1,
        help="Total number of nodes for multi-node downloading (default: 1)",
    )

    args = parser.parse_args()

    if args.rank < 0 or args.rank >= args.world_size:
        parser.error(f"--rank must be in [0, {args.world_size - 1}] for --world_size={args.world_size}")

    if args.use_aria2:
        if shutil.which("aria2c") is None:
            print("aria2c not found in PATH, falling back to Python downloader.")
            args.use_aria2 = False

    catalog_path = download_catalog(args.catalog_name, args.release, use_aria2=args.use_aria2)
    if catalog_path is None:
        print("Failed to download catalog, exiting.")
        return

    survey_type = "MRS" if args.catalog_name.startswith("MRS") else "LRS"

    download_tars(
        catalog_path=catalog_path,
        survey_type=survey_type,
        output_dir=args.output,
        max_rows=args.max_rows,
        release=args.release,
        rank=args.rank,
        world_size=args.world_size,
        use_aria2=args.use_aria2,
    )


if __name__ == "__main__":
    main()

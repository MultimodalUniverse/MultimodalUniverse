import argparse
import os

import requests
from bs4 import BeautifulSoup

parser = argparse.ArgumentParser(description="Download GALAH DR3 data")
parser.add_argument("--output_dir", type=str, default="./data", help="Output directory")
parser.add_argument(
    "--tiny",
    action="store_true",
    default=False,
    help="Download a tiny version of the data",
)
args = parser.parse_args()

os.makedirs(args.output_dir, exist_ok=True)

print("DOWNLOADING VAC, CATALOG, RESOLUTION MAPS, MISSING SPECTRA LIST")

urls = [
    # VAC for ages/parameters
    "https://cloud.datacentral.org.au/teamdata/GALAH/public/GALAH_DR3/GALAH_DR3_VAC_ages_v2.fits",
    # main catalog
    "https://cloud.datacentral.org.au/teamdata/GALAH/public/GALAH_DR3/GALAH_DR3_main_allstar_v2.fits",
    # resolution maps, one for each CCD
    "https://github.com/svenbuder/GALAH_DR3/raw/refs/heads/master/analysis/resolution_maps/ccd1_piv.fits",
    "https://github.com/svenbuder/GALAH_DR3/raw/refs/heads/master/analysis/resolution_maps/ccd2_piv.fits",
    "https://github.com/svenbuder/GALAH_DR3/raw/refs/heads/master/analysis/resolution_maps/ccd3_piv.fits",
    "https://github.com/svenbuder/GALAH_DR3/raw/refs/heads/master/analysis/resolution_maps/ccd4_piv.fits",
    # list of objects with missing spectra
    "https://cloud.datacentral.org.au/teamdata/GALAH/public/GALAH_DR3/spectra/GALAH_DR3_list_missing_reduced_spectra_v2.csv",
]

dl_string = " ".join([f'"{url}"' for url in urls])

os.system(f"aria2c -j16 -s16 -x16 -c -Z {dl_string} --dir={args.output_dir}")

print("DOWNLOADING SPECTRA")

os.makedirs(os.path.join(args.output_dir, "spectra"), exist_ok=True)
if args.tiny:
    print("Downloading a tiny version of the spectra files.")
    os.system(
        f'aria2c -j16 -s16 -x16 -c -Z "https://cloud.datacentral.org.au/teamdata/GALAH/public/GALAH_DR3/spectra/tar_files/151224_com.tar.gz" "https://cloud.datacentral.org.au/teamdata/GALAH/public/GALAH_DR3/spectra/tar_files/151224_com2.tar.gz" --dir={os.path.join(args.output_dir, "spectra")}'
    )
else:
    print("Downloading all spectra files. This will take a while.")

    base_url = "https://cloud.datacentral.org.au/teamdata/GALAH/public/GALAH_DR3/spectra/tar_files/"

    soup = BeautifulSoup(requests.get(base_url).content, "html.parser")

    dl_string = ""

    with open("filenames.txt", "w") as f:
        for link in soup.select('a[href*=".tar.gz"]'):
            dl_string += f'"{base_url}{link["href"]}" '

    os.system(
        f"aria2c -j16 -s16 -x16 -c -Z {dl_string} --dir={os.path.join(args.output_dir, 'spectra')}"
    )

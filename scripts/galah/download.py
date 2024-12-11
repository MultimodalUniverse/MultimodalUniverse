#!/bin/bash
import argparse
import os

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

os.system(
    f'aria2c -j16 -s16 -x16 -c -Z "https://cloud.datacentral.org.au/teamdata/GALAH/public/GALAH_DR3/GALAH_DR3_VAC_ages_v2.fits" "https://cloud.datacentral.org.au/teamdata/GALAH/public/GALAH_DR3/GALAH_DR3_main_allstar_v2.fits" "https://github.com/svenbuder/GALAH_DR3/raw/refs/heads/master/analysis/resolution_maps/ccd1_piv.fits" "https://github.com/svenbuder/GALAH_DR3/raw/refs/heads/master/analysis/resolution_maps/ccd2_piv.fits" "https://github.com/svenbuder/GALAH_DR3/raw/refs/heads/master/analysis/resolution_maps/ccd3_piv.fits" "https://github.com/svenbuder/GALAH_DR3/raw/refs/heads/master/analysis/resolution_maps/ccd4_piv.fits" "https://cloud.datacentral.org.au/teamdata/GALAH/public/GALAH_DR3/spectra/GALAH_DR3_list_missing_reduced_spectra_v2.csv" --dir={args.output_dir}'
)

print("DOWNLOADING SPECTRA")

os.makedirs(os.path.join(args.output_dir, "spectra"), exist_ok=True)
if args.tiny:
    print("Downloading a tiny version of the spectra files")
    os.system(
        f'aria2c -j16 -s16 -x16 -c -Z "https://cloud.datacentral.org.au/teamdata/GALAH/public/GALAH_DR3/spectra/tar_files/151224_com.tar.gz" "https://cloud.datacentral.org.au/teamdata/GALAH/public/GALAH_DR3/spectra/tar_files/151224_com2.tar.gz" --dir={os.path.join(args.output_dir, "spectra")}'
    )
else:
    print("Downloading all spectra files. This will take a while.")
    os.system(f"""
        curl -s -k "https://cloud.datacentral.org.au/teamdata/GALAH/public/GALAH_DR3/spectra/tar_files/" | grep -o 'href=".*">' | sed -e "s/href=\\"//g" | sed -e 's/">//g' | grep "^[0-9]*.*.tar.gz" > filelist.txt
        sed -i "s/^/https:\\/\\/cloud.datacentral.org.au\\/teamdata\\/GALAH\\/public\\/GALAH_DR3\\/spectra\\/tar_files\\//" filelist.txt
        aria2c -j16 -s16 -x16 -c -i filelist.txt --dir={os.path.join(args.output_dir, "spectra")}
    """)

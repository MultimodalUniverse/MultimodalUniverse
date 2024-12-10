from astroquery.mast import Observations
from astroquery.mast import Catalogs
from astropy.table import vstack
import requests
import time

from tqdm import tqdm
import argparse
from pathlib import Path

PIPELINE = 'KEPLER'
LONG_CADENCE = "Lightcurve Long Cadence"
SHORT_CADENCE = "Lightcurve Short Cadence"

# Kepler Quarters range from 0 to 17
QUARTERS = list(range(0, 18))


def batcher(seq, batch_size):
    return (seq[pos:pos + batch_size] for pos in range(0, len(seq), batch_size))


def main(args):
    QUARTER = args.quarter
    CADENCE = LONG_CADENCE if args.cadence == 'long' else SHORT_CADENCE
    CADENCE_SUFFIX = '_llc.fits' if args.cadence == 'long' else '_slc.fits'

    # If a specific quarter is provided, query for that quarter
    if QUARTER is not None:
        print(f"Querying for Quarter: {QUARTER}, Cadence: {'Long' if args.cadence == 'long' else 'Short'}")
        obs_table = Observations.query_criteria(provenance_name=PIPELINE,
                                                dataproduct_type='timeseries',
                                                dataRights='PUBLIC')
    else:
        # If no quarter specified, query all Kepler data
        obs_table = Observations.query_criteria(provenance_name=PIPELINE,
                                                dataproduct_type='timeseries',
                                                dataRights='PUBLIC')

    obs_table = Observations.filter_products(
        obs_table,
        productType="SCIENCE",
        description=CADENCE
    )
    print(f"Observation table: {len(obs_table)}")
    exit()

    # Only download a small sample of the data for testing purposes
    if args.tiny:
        products = Observations.get_product_list(obs_table[:10])
    else:
        # Retrieve the product list in batches to avoid timeout errors
        batch_size = 5000
        table_len = len(obs_table)
        print(f'Retrieving data product download links for {table_len} targets in batches of {batch_size}')
        try:
            product_list = [Observations.get_product_list(batch) for batch in
                            tqdm(batcher(obs_table, batch_size), total=table_len // batch_size)]
            products = vstack(product_list)
        except requests.exceptions.Timeout as e:
            print(e)
            print('Request timed out. Trying again...')
            time.sleep(3)
            product_list = [Observations.get_product_list(batch) for batch in
                            tqdm(batcher(obs_table, batch_size), total=table_len // batch_size)]
            products = vstack(product_list)

    # Download light curve files
    manifest = Observations.download_products(products, extension=CADENCE_SUFFIX, download_dir=args.output_path,
                                              flat=True)

    # Build a catalog based on the downloaded data
    catalog = manifest.to_pandas()
    catalog['productFilename'] = catalog['Local Path'].str.split('/', expand=True).iloc[:, -1:]
    catalog = catalog.merge(products.to_pandas(), on='productFilename')
    catalog = catalog[['Local Path', 'Status', 'productFilename', 'dataURI']]
    catalog.rename(columns={'dataURI': 'dataURL', 'Local Path': 'lc_path'}, inplace=True)
    catalog = catalog.merge(obs_table.to_pandas(), on='dataURL')

    # Only keep the targets for which the light curves were downloaded successfully
    catalog = catalog[catalog['Status'] == 'COMPLETE'].reset_index(drop=True)
    catalog.rename(columns={'s_ra': 'RA', 's_dec': 'DEC'}, inplace=True)
    catalog['quarter'] = QUARTER if QUARTER is not None else 'All'
    catalog['pipeline'] = PIPELINE
    catalog['cadence'] = args.cadence

    # Replace relative with absolute path
    catalog['lc_path'] = catalog['lc_path'].apply(lambda x: Path(x).resolve())

    catalog = catalog[['lc_path', 'target_name', 'RA', 'DEC', 'quarter', 'pipeline', 'cadence']]

    # Save the catalog
    output_filename = f'kepler_lc_catalog_quarter_{QUARTER}_{args.cadence}_cadence.csv' if QUARTER is not None else f'kepler_lc_catalog_all_quarters_{args.cadence}_cadence.csv'
    catalog.to_csv(Path(args.output_path) / output_filename, index=None)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Downloads Kepler light curve data to a user-provided endpoint.")

    parser.add_argument('output_path', type=str, help='Path to save the data')
    parser.add_argument('--tiny', action='store_true', help='Use a tiny subset of the data for testing')
    parser.add_argument('-q', '--quarter', type=int, default=None,
                        help="Kepler Quarter to download (0-17). If not specified, downloads all quarters.")
    parser.add_argument('-c', '--cadence', type=str, choices=['long', 'short'], default='long',
                        help="Choose light curve cadence. 'long' (default) or 'short'.")

    args = parser.parse_args()

    # Create output directory if it doesn't exist
    output_path = Path(args.output_path)
    output_path.mkdir(parents=True, exist_ok=True)
    print(f'Output directory: {output_path}')

    main(args)
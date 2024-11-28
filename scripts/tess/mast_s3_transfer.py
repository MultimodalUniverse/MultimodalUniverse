from astroquery.mast import Observations
from astroquery.mast import Catalogs
from astropy.table import vstack
import requests
import time

from tqdm import tqdm
import argparse
from pathlib import Path

PIPELINE = 'TESS-SPOC'
CADENCE = [120.0,120.0]

# TESS Sector numbers for Primary Mission, Extended Mission 1, and Extended Mission 2
PM = list(range(1, 26 + 1))
EM1 = list(range(27, 55 + 1))
EM2 = list(range(56, 96 + 1))

def batcher(seq, batch_size):
    return (seq[pos:pos + batch_size] for pos in range(0, len(seq), batch_size))

def main(args):
    SECTOR = args.sector

    obs_table = Observations.query_criteria(provenance_name=PIPELINE,
                                            #t_exptime=CADENCE,
                                            sequence_number=SECTOR,
                                            dataproduct_type='timeseries',
                                            dataRights='PUBLIC')

    # Only download a small sample of the data for testing purposes
    if args.tiny:
        products = Observations.get_product_list(obs_table[:10])
    else:
        # Retrieve the product list in batches because the query is often too large for MAST to handle in one go, resulting in time out errors.
        batch_size = 5000
        table_len = len(obs_table)
        print('Retrieving data product download links for {} targets in batches of {}'.format(table_len, batch_size))
        try:
            product_list = [Observations.get_product_list(batch) for batch in tqdm(batcher(obs_table, batch_size), total = table_len // batch_size)]
            products = vstack(product_list)
        except requests.exceptions.Timeout as e:
            print(e)
            print('Request timed out. Trying again...')
            time.sleep(3)
            product_list = [Observations.get_product_list(batch) for batch in tqdm(batcher(obs_table, batch_size), total = table_len // batch_size)]
            products = vstack(product_list)

    manifest = Observations.download_products(products, extension="_lc.fits", download_dir=args.output_path, flat=True)

    # Build a catalog based on the downloaded data
    catalog = manifest.to_pandas()
    catalog['productFilename'] = catalog['Local Path'].str.split('/', expand=True).iloc[:,-1:]
    catalog = catalog.merge(products.to_pandas(), on='productFilename')
    catalog = catalog[['Local Path','Status','productFilename', 'dataURI']]
    catalog.rename(columns={'dataURI': 'dataURL', 'Local Path': 'lc_path'}, inplace=True)
    catalog = catalog.merge(obs_table.to_pandas(), on='dataURL')

    # Only keep the targets for which the light curves were downloaded succesfully
    catalog = catalog[catalog['Status']=='COMPLETE'].reset_index(drop=True)
    catalog.rename(columns={'s_ra': 'RA','s_dec': 'DEC'}, inplace=True)
    catalog['sector'] = SECTOR
    catalog['pipeline'] = PIPELINE

    # Optional - replace relative with absolute path
    catalog['lc_path'] = catalog['lc_path'].apply(lambda x: Path(x).resolve()) 
    
    catalog = catalog[['lc_path','target_name','RA','DEC','sector','pipeline']]

    # TODO: query TESS Input Catalog (TIC) for all other available parameters:
    # catalog_data = Catalogs.query_criteria(catalog="TIC",Tmag=[10,10.1], objType="STAR")

    # Save the catalog
    catalog.to_csv(args.output_path + '/tess_lc_catalog_sector_' + str(SECTOR) + '.csv', index=None)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Downloads TESS data to a user-provided endpoint.")

    # TODO: implement pipeline argument. Note: QLP Pipeline requires different download script.
    # parser.add_argument("TESS Pipeline", type=str, help="TESS Data Pipeline to download.")
    # TODO: add download option per mission cycle (Primary Mission, Extended Mission 1, Extended Mission 2)

    parser.add_argument('output_path', type=str, help='Path to save the data')
    parser.add_argument('--tiny', action='store_true', help='Use a tiny subset of the data for testing')

    #TODO: adjust urllib3 number of retries in case of a download failure (which is common in mast/astroquery)
    parser.add_argument('-s', '--sector', type=int, default=64, help="TESS Sector to download.")

    args = parser.parse_args()

    if not Path(args.output_path).exists():
        Path(args.output_path).mkdir(parents=True)
        print('Created directory:', args.output_path)
    else:
        print('Output directory already exists:', args.output_path)
    
    main(args)
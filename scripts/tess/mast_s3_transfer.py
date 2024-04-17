from astroquery.mast import Observations
from astroquery.mast import Catalogs
import argparse
from pathlib import Path

PIPELINE = 'TESS-SPOC'
CADENCE = [120.0,120.0]

# TODO: add support to download per mission, might give download problems though due to large sizes.
# Requires split or loop probably.

# TESS Sector numbers for Primary Mission, Extended Mission 1, and Extended Mission 2
PM = list(range(1, 26 + 1))
EM1 = list(range(27, 55 + 1))
EM2 = list(range(56, 96 + 1))


def main(args):
    # TESS Input Catalog
    # Catalogs.enable_cloud_dataset()
    # catalog_data = Catalogs.query_criteria(catalog="TIC", Tmag=[10,10.1], objType="STAR")
    # TODO: save catalog_data (i.e. the TIC)to a file

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
        products = Observations.get_product_list(obs_table)

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
    catalog = catalog[['lc_path','target_name','RA','DEC']]

    # TODO: query  TESS Input Catalog (TIC) for all other available parameters:
    # catalog_data = Catalogs.query_criteria(catalog="TIC",Tmag=[10,10.1], objType="STAR")
    # Save our "Catalog" to a file

    # Save the catalog
    catalog.to_csv(args.output_path + '/tess_lc_catalog_sector_' + str(SECTOR) + '.csv', index=None)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Downloads TESS data to a user-provided endpoint.")
    # TODO: implement the following arguments

    # QLP Pipeline requires different download script
    # parser.add_argument("TESS Pipeline", type=str, help="TESS Data Pipeline to download.")
    # parser.add_argument("TESS Cadence", type=str, help="Cadence to use for the download.")

    parser.add_argument('output_path', type=str, help='Path to save the data')
    parser.add_argument('--tiny', action='store_true', help='Use a tiny subset of the data for testing')

    #TODO: add download option per mission cycle
    parser.add_argument('-s', '--sector', type=int, default=64, help="TESS Sector to download.")

    args = parser.parse_args()

    if not Path(args.output_path).exists():
        Path(args.output_path).mkdir(parents=True)
        print('Created directory:', args.output_path)
    else:
        print('Output directory already exists:', args.output_path)
    
    main(args)
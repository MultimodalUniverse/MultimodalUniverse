from astroquery.mast import Observations
from astroquery.mast import Catalogs
import argparse

Observations.enable_cloud_dataset()

PIPELINE = ['SPOC']
SECTOR = 64
CADENCE = [120.0,120.0]


def main(args):
    # TESS Input Catalog
    # Catalogs.enable_cloud_dataset()
    # catalog_data = Catalogs.query_criteria(catalog="TIC", Tmag=[10,10.1], objType="STAR")
    # TODO: save catalog_data (i.e. the TIC)to a file

    obs_table = Observations.query_criteria(obs_collection=['TESS'], provenance_name=PIPELINE, t_exptime=CADENCE, sequence_number=SECTOR, dataRights='PUBLIC')
    products = Observations.get_product_list(obs_table)
    filtered = Observations.filter_products(products,
                                            productSubGroupDescription='LC')
    filtered = filtered[:10] # For testing purposes
    
    # In case we need the explicit AWS S3 URIs
    # s3_uris = Observations.get_cloud_uris(filtered)

    # TODO: add options to specify the download folder
    manifest = Observations.download_products(filtered, cloud_only=True)

    # Save our "Catalog" to a file
    filtered['Local_Path'] = manifest['Local Path']
    # TODO: add RA and DEC to the catalog
    filtered.write('./mastDownload/TESS/tess_lc_manifest.csv', format='csv')



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Transfer data from DESI to user-provided endpoint.")
    # TODO: implement the following arguments
    # parser.add_argument("TESS Sector", type=str, help="TESS Sector to download.")
    # parser.add_argument("TESS Pipeline", type=str, help="TESS Data Pipeline to download.")
    # parser.add_argument("TESS Cadence", type=str, help="Cadence to use for the download.")
    args = parser.parse_args()
    main(args)
import os
import numpy as np
import argparse
import urllib.request
from astropy.table import Table, unique
from globus_sdk import TransferClient, TransferData, NativeAppAuthClient, AccessTokenAuthorizer
from globus_sdk.scopes import TransferScopes

# Main catalog file
SDSS_CATALOG_URL = "https://data.sdss.org/sas/dr17/sdss/spectro/redux/specObj-dr17.fits"

# GLOBUS endpoint ID for SDSS
SDSS_GLOBUS_ENDPOINT = "f8362eaf-fc40-451c-8c44-50b71ec7f247"

SDSS_BASE_PATH ={'sdss  ': '/dr17/sdss/spectro/redux/26/spectra/lite/',
                 'segue1': '/dr17/sdss/spectro/redux/103/spectra/lite/',
                 'segue2': '/dr17/sdss/spectro/redux/104/spectra/lite/',
                 'boss  ': '/dr17/boss/spectro/redux/v5_13_0/spectra/lite/',
                 'eboss ': '/dr17/eboss/spectro/redux/v5_13_0/spectra/lite/'
                 }

def main(args):
    # Globus endpoint IDs
    source_endpoint_id = SDSS_GLOBUS_ENDPOINT
    destination_endpoint_id = args.destination_endpoint_id
    # User-provided endpoint path
    destination_path = args.destination_path

    # Retrieve the DESI tilepix file
    if not os.path.exists(SDSS_CATALOG_URL.split("/")[-1]):
        print("Downloading the SDSS catalog through HTTPS...")
        dowloaded_file, msg = urllib.request.urlretrieve(SDSS_CATALOG_URL)
        print("Download complete.")

    # Opening the file and extracting all 
    catalog = Table.read(SDSS_CATALOG_URL.split("/")[-1])
    catalog = catalog["PLATE", "MJD", "FIBERID", "SURVEY"]
    catalog = unique(catalog, keys=["PLATE", "MJD", "FIBERID", "SURVEY"])
        
    # this is the tutorial client ID
    # replace this string with your ID for production use
    CLIENT_ID = "61338d24-54d5-408f-a10d-66c06b59f6d2"
    client = NativeAppAuthClient(CLIENT_ID)

    # Start the login flow
    client.oauth2_start_flow(requested_scopes=[TransferScopes.all,
                                               f"urn:globus:auth:scope:transfer.api.globus.org:all[*https://auth.globus.org/scopes/{destination_endpoint_id}/data_access]"
                                               ])

    # Get the authorization URL
    authorize_url = client.oauth2_get_authorize_url()

    # Print the authorization URL and ask the user to visit it
    print(f'Please go to this URL and login: {authorize_url}')

    # Get the authorization code from the user
    auth_code = input('Please enter the code you get after login here: ').strip()

    # Exchange the authorization code for a token
    token_response = client.oauth2_exchange_code_for_tokens(auth_code)

    globus_transfer_data = token_response.by_resource_server["transfer.api.globus.org"]

    # most specifically, you want these tokens as strings
    TRANSFER_TOKEN = globus_transfer_data["access_token"]
    
    # Use the access token to create a Globus transfer client
    authorizer = AccessTokenAuthorizer(TRANSFER_TOKEN)
    transfer_client = TransferClient(authorizer=authorizer)

    # Breaking down the download into chunks 
    chunk_size = 200_000
    n_chunks = int(np.ceil(len(catalog)/chunk_size))
    for chunk in range(n_chunks):
        cat = catalog[chunk*chunk_size:min((chunk+1)*chunk_size, len(catalog))]

        # Create a transfer data object
        transfer_data = TransferData(transfer_client,
                                    source_endpoint_id,
                                    destination_endpoint_id,
                                    label="SDSS Transfer %i/%i" % (chunk+1, n_chunks),
                                    sync_level="size")
        
        # Add item for all healpix pixels and all surveys
        n_files = 1
        for row in cat:
            plate = row["PLATE"]
            mjd = row["MJD"]
            fiberid = row["FIBERID"]
            filename = "spec-%s-%i-%s.fits" % (
                str(plate).zfill(4),
                mjd,
                str(fiberid).zfill(4),
            )
            source_path = SDSS_BASE_PATH[row["SURVEY"]]+str(plate).zfill(4)+"/"
            transfer_data.add_item(source_path+filename, 
                                destination_path+row["SURVEY"].strip()+'/'+str(plate).zfill(4)+"/"+filename)
            n_files += 1
            
        print("Submitting transfer request for %d files..." % n_files)
        # Initiate the transfer
        transfer_result = transfer_client.submit_transfer(transfer_data)

        # Get the transfer ID
        transfer_id = transfer_result["task_id"]
        print(f"Transfer {chunk}/{n_chunks} submitted with ID: {transfer_id}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Transfer data from DESI to user-provided endpoint.")
    parser.add_argument("destination_endpoint_id", type=str, help="The destination Globus endpoint ID.")
    parser.add_argument("destination_path", type=str, help="The destination path on the endpoint.")
    args = parser.parse_args()
    main(args)
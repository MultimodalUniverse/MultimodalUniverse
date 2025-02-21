import os
import numpy as np
import argparse
import urllib.request
from astropy.table import Table, unique
from globus_sdk import TransferClient, TransferData, NativeAppAuthClient, AccessTokenAuthorizer


DESI_TILEPIX_URL = "https://data.desi.lbl.gov/public/edr/spectro/redux/fuji/healpix/tilepix.fits"
DESI_GLOBUS_ENDPOINT = "6b4e1f6a-e600-11ed-9b9b-c9bb788c490e"

# All the files to transfer in addition to spectra
DESI_TRANSFER_ITEMS = [
    "/edr/spectro/redux/fuji/zcatalog/zall-pix-fuji.fits"
]

# Select which surveys to transfer
DESI_SURVEYS = [
    "sv3"
]


def main(args):
    # Globus endpoint IDs
    source_endpoint_id = "6b4e1f6a-e600-11ed-9b9b-c9bb788c490e"
    destination_endpoint_id = args.destination_endpoint_id
    # User-provided endpoint path
    destination_path = args.destination_path

    # Retrieve the DESI tilepix file
    if not os.path.exists("tilepix.fits"):
        print("Downloading DESI tilepix file...")
        urllib.request.urlretrieve(DESI_TILEPIX_URL, "tilepix.fits")
        print("Download complete.")

    # Opening the file and extracting all 
    tilepix = Table.read("tilepix.fits")
    # Only keep the rows that correspond to SV3, only keep the columns ["HEALPIX", "SURVEY", "PROGRAM"], and remove duplicates
    tilepix = tilepix[np.any([tilepix["SURVEY"] == s for s in DESI_SURVEYS], axis=0)]
    tilepix = tilepix["HEALPIX", "SURVEY", "PROGRAM"]
    tilepix = unique(tilepix, keys=["HEALPIX", "SURVEY", "PROGRAM"])
        
    # this is the tutorial client ID
    # replace this string with your ID for production use
    CLIENT_ID = "61338d24-54d5-408f-a10d-66c06b59f6d2"
    client = NativeAppAuthClient(CLIENT_ID)

    # Start the login flow
    client.oauth2_start_flow()

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

    # Create a transfer data object
    transfer_data = TransferData(transfer_client,
                                source_endpoint_id,
                                destination_endpoint_id,
                                label="DESI EDR Transfer",
                                sync_level="size")

    # Add the source path to transfer data
    for item in DESI_TRANSFER_ITEMS:
        transfer_data.add_item(item, destination_path+"/"+item.split("/")[-1])
    
    # Add item for all healpix pixels and all surveys
    n_files = 1
    for row in tilepix:
        healpix = row["HEALPIX"]
        survey = row["SURVEY"]
        program = row["PROGRAM"]
        short_pix = str(healpix)[:-2]
        transfer_data.add_item(f"/edr/spectro/redux/fuji/healpix/{survey}/{program}/{short_pix}/{healpix}/coadd-{survey}-{program}-{healpix}.fits", 
                               destination_path+f"/coadd-{survey}-{program}-{healpix}.fits")
        n_files += 1
        
    print("Submitting transfer request for %d files..." % n_files)
    # Initiate the transfer
    transfer_result = transfer_client.submit_transfer(transfer_data)

    # Get the transfer ID
    transfer_id = transfer_result["task_id"]
    print(f"Transfer submitted with ID: {transfer_id}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Transfer data from DESI to user-provided endpoint.")
    parser.add_argument("destination_endpoint_id", type=str, help="The destination Globus endpoint ID.")
    parser.add_argument("destination_path", type=str, help="The destination path on the endpoint.")
    args = parser.parse_args()
    main(args)

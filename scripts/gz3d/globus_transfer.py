
import argparse
import pathlib
import urllib
import requests
from bs4 import BeautifulSoup
import re

from astropy.io import fits
from globus_sdk import TransferClient, TransferData, NativeAppAuthClient, AccessTokenAuthorizer
from globus_sdk.scopes import TransferScopes

from .build_parent_sample import GZ3D_URL, build_filelist

# GLOBUS endpoint ID for public SDSS data
SDSS_GLOBUS_ENDPOINT = "f8362eaf-fc40-451c-8c44-50b71ec7f247"

def transfer_data(destination_endpoint_id: str, destination_filepath: str = '.',
                  client_id: str = None, limit: int = None, product: str = 'cubes'):
    """ Transfer SDSS GZ3D data via Globus """

    # Globus endpoint IDs
    source_endpoint_id = SDSS_GLOBUS_ENDPOINT

    # your globus client id
    client = NativeAppAuthClient(client_id)

    # Start the login flow
    scope = f"urn:globus:auth:scope:transfer.api.globus.org:all[*https://auth.globus.org/scopes/{destination_endpoint_id}/data_access]"
    client.oauth2_start_flow(requested_scopes=[TransferScopes.all, scope])

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
    transfer_data = TransferData(
        transfer_client,
        source_endpoint_id,
        destination_endpoint_id,
        label="SDSS Transfer" ,
        sync_level="size"
        )

    # setup products
    files = build_filelist(GZ3D_URL, limit=limit)

    n_files = len(files)

    for file in files:
        transfer_data.add_item(str(file), destination_filepath + "/" + str(file.split("/")[-1]))

    print(f"Submitting transfer request for {n_files} files...")
    # Initiate the transfer
    transfer_result = transfer_client.submit_transfer(transfer_data)

    # Get the transfer ID
    transfer_id = transfer_result["task_id"]
    print(f"Transfer submitted with ID: {transfer_id}")


if __name__ == "__main__":

    # parse cli arguments
    parser = argparse.ArgumentParser(description="Transfer data from public SDSS endpoint to a user-provided endpoint.")
    parser.add_argument("-d", "--destination_endpoint_id", type=str, help="The destination Globus endpoint ID.")
    parser.add_argument("-o", "--destination_path", type=str, help="The destination path on the endpoint.")
    parser.add_argument("-c", "--client_id", type=str, help="Your Globus client id")
    parser.add_argument("-l", "--limit", type=int, help="Limit of source files to download.")

    args = parser.parse_args()

    # transfer data via globus
    transfer_data(
        args.destination_endpoint_id, 
        args.destination_path, 
        client_id=args.client_id,
        limit=args.limit, 
        product=args.product
    )
    
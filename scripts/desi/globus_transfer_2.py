import os
import numpy as np
import argparse
import urllib.request
from astropy.table import Table, unique
import globus_sdk # Import the main SDK
from globus_sdk.scopes import TransferScopes # Import TransferScopes

DESI_TILEPIX_URL = "https://data.desi.lbl.gov/public/dr1/spectro/redux/iron/healpix/tilepix.fits"
DESI_GLOBUS_ENDPOINT = "6b4e1f6a-e600-11ed-9b9b-c9bb788c490e"

# All the files to transfer in addition to spectra
DESI_TRANSFER_ITEMS = [
    "/dr1/spectro/redux/iron/zcatalog/v1/zall-pix-iron.fits"
]

# Tutorial client ID - replace with your own for production
CLIENT_ID = "61338d24-54d5-408f-a10d-66c06b59f6d2"

# Globus Authentication Helper Function
def login_and_get_transfer_client(native_client, *, scopes=TransferScopes.all):
    """
    Performs the Globus Native App auth flow and returns a TransferClient.
    Handles prompting the user for the auth code.
    Can be called with specific scopes if needed (e.g., for consent).
    """
    native_client.oauth2_start_flow(requested_scopes=scopes)
    authorize_url = native_client.oauth2_get_authorize_url()
    print(f'Please go to this URL and login: {authorize_url}')
    auth_code = input('Please enter the code you get after login here: ').strip()
    token_response = native_client.oauth2_exchange_code_for_tokens(auth_code)
    globus_transfer_data = token_response.by_resource_server["transfer.api.globus.org"]
    
    # Use the access token to create a Globus transfer client
    authorizer = globus_sdk.AccessTokenAuthorizer(globus_transfer_data["access_token"])
    transfer_client = globus_sdk.TransferClient(authorizer=authorizer)
    return transfer_client

# Main Script Logic
def main(args):
    # Globus endpoint IDs
    source_endpoint_id = DESI_GLOBUS_ENDPOINT
    destination_endpoint_id = args.destination_endpoint_id
    # User-provided endpoint path
    destination_path = args.destination_path.rstrip('/') # Ensure no trailing slash for consistency
    
    # Retrieve the DESI tilepix file
    if not os.path.exists("tilepix.fits"):
        print("Downloading DESI tilepix file...")
        urllib.request.urlretrieve(DESI_TILEPIX_URL, "tilepix.fits")
        print("Download complete.")
        
    # Opening the file and extracting relevant data
    print("Reading tilepix data...")
    tilepix = Table.read("tilepix.fits")
    # Filter rows based on args.surveys, keep specific columns, and remove duplicates
    tilepix = tilepix[np.any([tilepix["SURVEY"] == s for s in args.surveys], axis=0)]
    tilepix = tilepix["HEALPIX", "SURVEY", "PROGRAM"]
    tilepix = unique(tilepix, keys=["HEALPIX", "SURVEY", "PROGRAM"])
    print(f"Found {len(tilepix)} unique HEALPIX entries for surveys: {', '.join(args.surveys)}")
    
    # --- Globus Authentication and Transfer Setup ---
    native_client = globus_sdk.NativeAppAuthClient(CLIENT_ID)
    
    # Initial login attempt
    print("\nStarting Globus authentication...")
    transfer_client = login_and_get_transfer_client(native_client) # Uses default TransferScopes.all
    
    # Create a transfer data object
    transfer_data = globus_sdk.TransferData(transfer_client=transfer_client, # Pass client here
                                            source_endpoint=source_endpoint_id,
                                            destination_endpoint=destination_endpoint_id,
                                            label="DESI Transfer",
                                            sync_level="checksum") # Changed sync_level to checksum for robustness
                                            
    # Add the static transfer items
    print("Adding static files to transfer batch...")
    for item in DESI_TRANSFER_ITEMS:
        dest_filename = os.path.basename(item)
        transfer_data.add_item(item, os.path.join(destination_path, dest_filename))
        
    # Add items for all healpix pixels and surveys
    print("Adding HEALPIX coadd files to transfer batch...")
    n_files = len(DESI_TRANSFER_ITEMS) # Start count from static files
    for row in tilepix:
        healpix = row["HEALPIX"]
        survey = row["SURVEY"]
        program = row["PROGRAM"]
        # Construct source path based on DESI structure
        short_pix = str(healpix)[:-2] # Directory level is healpix without last 2 digits
        source_path = f"/dr1/spectro/redux/iron/healpix/{survey}/{program}/{short_pix}/{healpix}/coadd-{survey}-{program}-{healpix}.fits"
        dest_filename = f"coadd-{survey}-{program}-{healpix}.fits"
        transfer_data.add_item(source_path, os.path.join(destination_path, dest_filename))
        n_files += 1
        
    print(f"\nPrepared transfer request for {n_files} files.")
    
    # --- Submit Transfer with Consent Handling ---
    def do_submit(client, tdata):
        """Helper function to submit the transfer."""
        print("Submitting transfer request...")
        transfer_result = client.submit_transfer(tdata)
        transfer_id = transfer_result["task_id"]
        print(f"Transfer submitted successfully!")
        print(f"Monitor the transfer task here: https://app.globus.org/activity/{transfer_id}")
        return transfer_id
        
    try:
        # First attempt to submit
        transfer_id = do_submit(transfer_client, transfer_data)
    except globus_sdk.TransferAPIError as err:
        # Check if it's a ConsentRequired error
        if err.info.consent_required:
            print("\n=== Consent Required ===")
            print("The Globus transfer requires additional consent for the endpoints involved.")
            print("You need to log in again to grant the necessary permissions.")
            
            # Get required scopes from the error object
            required_scopes = err.info.consent_required.required_scopes
            
            # Re-authenticate with the required scopes
            transfer_client = login_and_get_transfer_client(native_client, scopes=required_scopes)
            
            # Update the transfer_data object with the new client
            # (Necessary because the client is associated with the TransferData upon creation)
            transfer_data.transfer_client = transfer_client
            
            # Retry the submission (without further error handling here)
            print("\nRetrying transfer submission with new consent...")
            transfer_id = do_submit(transfer_client, transfer_data)
        else:
            # If it's a different Globus API error, re-raise it
            print(f"\nAn unexpected Globus Transfer API error occurred: {err}")
            raise err

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Transfer data from DESI to user-provided endpoint.")
    parser.add_argument("destination_endpoint_id", type=str, help="The destination Globus endpoint ID.")
    parser.add_argument("destination_path", type=str, help="The destination path on the endpoint.")
    parser.add_argument("--surveys", type=str, nargs="+", help="Space delimited list of surveys to transfer.", default=["main"])
    args = parser.parse_args()
    main(args)
import os
import argparse
import requests
import tarfile

def main(args):
    
    record_id = "7317476"
    file_name = "yse_dr1_zenodo.tar.gz"

    # Construct the URL to download the file from Zenodo
    url = f"https://zenodo.org/record/{record_id}/files/{file_name}"
    
    # Send a GET request to the Zenodo URL to download the file
    response = requests.get(url, stream=True)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Open a file in binary write mode to save the downloaded content
        with open(args.destination_path+file_name, 'wb') as f:
            # Write tar.gz file to destination folder
            f.write(response.content)

        # Unzip tar.gz file
        tar = tarfile.open(args.destination_path+file_name, "r:gz")
        tar.extractall(args.destination_path)
        tar.close()
        
        # Remove tar.gz file
        os.remove(args.destination_path+file_name)
        
        # Print a success message if the file is downloaded successfully
        print(f"File downloaded successfully to {args.destination_path}")
    else:
        # Print an error message if the download fails
        print("Failed to download file")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Transfer data from YSE DR1 to user-provided destination folder.")
    parser.add_argument("destination_path", type=str, help="The destination path to download and unzip the data into.")
    args = parser.parse_args()
    main(args)
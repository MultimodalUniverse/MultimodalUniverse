import argparse
import os
import tarfile

import requests


def main(args):
    file_name = "cfa_snII_lightcurvesndstars.june2017.tar"
    # Construct the URL to download the file from Zenodo
    url = f"https://lweb.cfa.harvard.edu/supernova/fmalcolm2017/{file_name}"

    # Send a GET request to the Zenodo URL to download the file
    response = requests.get(url, stream=True)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Open a file in binary write mode to save the downloaded content
        with open(args.destination_path + file_name, "wb") as f:
            # Write tar.gz file to destination folder
            f.write(response.content)

        # Unzip tar.gz file
        tar = tarfile.open(args.destination_path + file_name)
        tar.extractall(args.destination_path)
        tar.close()

        # Remove tar.gz file
        os.remove(args.destination_path + file_name)

        # Print a success message if the file is downloaded successfully
        print(f"File downloaded successfully to {args.destination_path}")
        os.makedirs(f"{args.destination_path}CFA_SNII", exist_ok=True)

        os.rename(
            f"{args.destination_path}CFA_SNII_STDSYSTEM_LC.txt",
            f"{args.destination_path}CFA_SNII/STDSYSTEM_LC.txt",
        )
        os.rename(
            f"{args.destination_path}CFA_SNII_NIR_LC.txt",
            f"{args.destination_path}CFA_SNII/NIR_LC.txt",
        )
        os.remove(f"{args.destination_path}CFA_SNII_NATSYSTEM_LC.txt")
        os.remove(f"{args.destination_path}CFA_SNII_STDSYSTEM_STARS.txt")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Transfer CfA SNII data to user-provided destination folder."
    )
    parser.add_argument(
        "destination_path",
        type=str,
        help="The destination path to download and unzip the data into.",
    )
    args = parser.parse_args()
    main(args)

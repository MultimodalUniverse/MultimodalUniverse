import os
import shutil
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

    # Set the path to the archive and extracted archive
    archive_path = os.path.join(args.temp_download_path, file_name)
    ext_archive_path = os.path.join(args.temp_download_path, file_name.split('.')[0])

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        os.makedirs(args.temp_download_path)
        # Open a file in binary write mode to save the downloaded content
        with open(archive_path, 'wb') as f:
            # Write tar.gz file to temporary download directory
            f.write(response.content)

        # Unzip tar.gz file
        tar = tarfile.open(archive_path, "r:gz")
        tar.extractall(args.temp_download_path)
        tar.close()

        # Move the extracted files to the destination path
        os.rename(ext_archive_path, args.destination_path)

        # Remove the temporary download directory and its contents (tar.gz file)
        if not args.dirty:
            shutil.rmtree(args.temp_download_path)
        
        # Print a success message if the file is downloaded successfully
        print(f"File downloaded successfully to {args.destination_path}")

        # Change spaces to hyphens for columns in arg hyphenate-cols in snana.dat files
        hyphenate_cols = args.hyphenate_cols
        if isinstance(hyphenate_cols, list) and len(hyphenate_cols) > 0:
            files = os.listdir(args.destination_path)
            for file in files:
                file_path = os.path.join(args.destination_path, file)
                with open(file_path, 'r') as f:
                    lines = f.readlines()
                    for i in range(len(lines)):
                        line = lines[i].split(': ')
                        line[0] = line[0].replace(' ', '_')
                        if line[0] in hyphenate_cols:
                            line[1] = line[1].replace(' ', '-')
                        lines[i] = ': '.join(line)
                with open(file_path, 'w') as f:
                    f.writelines(lines)
    else:
        # Print an error message if the download fails
        print("Failed to download file")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Transfer data from YSE DR1 to user-provided destination folder.")
    parser.add_argument("--temp_download_path", type=str, help="The temporary path to download the data archive into.", default="./yse_data_temp")
    parser.add_argument("--destination_path", type=str, help="The destination path to extract the data into.", default="./yse_data_orig")
    parser.add_argument('--tiny', action="store_true", help='Use a small subset of the data for testing')  # NOTE: NOT IMPLEMENTED (YSE DR1 is already small)
    parser.add_argument('--dirty', action="store_true", help='Do not remove the downloaded data archive')
    parser.add_argument('-n', '--hyphenate-cols', nargs='+', default=['SPEC_CLASS', 'SPEC_CLASS_BROAD', 'PARSNIP_PRED', 'SUPERRAENN_PRED'])
    args = parser.parse_args()
    main(args)

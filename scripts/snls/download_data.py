import os
import argparse
import requests
import tarfile
import requests
import os
def download_text_file(url, file_path):
    response = requests.get(url)
    if response.status_code == 200:
        with open(file_path, 'w') as file:
            file.write(response.text)
    else:
        print(f"Failed to download text file. Status code: {response.status_code}")

def read_text_file(file_path):
    with open(file_path, 'r') as file:
        contents = file.read()
        return contents
    



def main(args):

    # Construct the URL to download 
    url = 'https://raw.githubusercontent.com/PantheonPlusSH0ES/DataRelease/main/Pantheon%2B_Data/1_DATA/photometry/JLA2014_SNLS_DS17/'
    
    #name of files
    names=read_text_file('SNLS_LIST.txt').split('\n')[:-1]

    if not os.path.exists(os.path.join(args.destination_path,'snls')):
        os.makedirs(os.path.join(args.destination_path,'snls'))
        
    for file in names:
        download_text_file(url+file,os.path.join(args.destination_path,'snls',file))
    
 

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Transfer data from SNLS to user-provided destination folder.")
    parser.add_argument("destination_path", type=str, help="The destination path to download and unzip the data into.")
    args = parser.parse_args()
    main(args)

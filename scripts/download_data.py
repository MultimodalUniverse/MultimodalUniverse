import argparse
import os
import tarfile

import requests

def download_tar_file(url, destination_path, file_name):
    response = requests.get(url+file_name, stream=True)
    if response.status_code == 200:
        with open(destination_path + file_name, "wb") as f:
            f.write(response.content)
    else:
        raise ValueError(f"Failed to download text file. Status code: {response.status_code}")

def download_text_file(url, destination_path):
    response = requests.get(url)
    if response.status_code == 200:
        with open(destination_path, 'w') as file:
            file.write(response.text)
    else:
        raise ValueError(f"Failed to download text file. Status code: {response.status_code}")

def read_text_file(file_path):
    with open(file_path, 'r') as file:
        contents = file.read()
        return contents

def extract_tar_file(destination_path, file_name, *tar_args):
    tar = tarfile.open(os.path.join(destination_path, file_name))
    tar.extractall(destination_path)
    tar.close()
    os.remove(destination_path + file_name)

def cfa_snII_logic(args):
    download_tar_file(urls[args.dataset], args.destination_path, file_names[args.dataset])
    extract_tar_file(args.destination_path, file_names[args.dataset])
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

def csp_dr3_logic(args):
    download_tar_file(urls[args.dataset], args.destination_path, file_names[args.dataset])
    extract_tar_file(args.destination_path, file_names[args.dataset], 'r:gz')
    os.rename(f'{args.destination_path}/DR3', f'{args.destination_path}/CSPDR3')
    os.remove(f'{args.destination_path}/CSPDR3/README')
    os.remove(f'{args.destination_path}/CSPDR3/._tab1.dat')
    os.remove(f'{args.destination_path}/CSPDR3/.format_data.py.swp')

def github_logic(args):
    #name of files
    names=read_text_file(file_names[args.dataset]).split('\n')[:-1]
    for file in names:
        download_text_file(urls[args.dataset]+file, os.path.join(args.destination_path, dir_name[args.dataset], file))

SNANA_DATASETS = ('des_y3_sne_ia', 'foundation', 'ps1_sne_ia', 'snls', 'swift_sne_ia')
file_names = {
        'cfa_snII': "cfa_snII_lightcurvesndstars.june2017.tar",
        'csp_dr3': "CSP_Photometry_DR3.tgz",
        'des_y3_sne_ia': 'DES3YR_DES_LIST.txt',
        'foundation': 'Foundation_DR1_list.txt',
        'ps1_sne_ia': 'PS1_LIST.txt',
        'snls': 'SNLS_LIST.txt',
        'swift_sne_ia': 'SWIFT_LIST.txt',
}
PPlusSHOES = 'https://raw.githubusercontent.com/PantheonPlusSH0ES/DataRelease/main/Pantheon%2B_Data/1_DATA/photometry/'
urls = {
        'cfa_snII': "https://lweb.cfa.harvard.edu/supernova/fmalcolm2017/",
        'csp_dr3': "https://csp.obs.carnegiescience.edu/data/",
        'des_y3_sne_ia': PPlusSHOES+'DES3YR_DES_COMBINED_TEXT/',
        'foundation': 'https://raw.githubusercontent.com/djones1040/Foundation_DR1/master/Foundation_DR1/',
        'ps1_sne_ia': PPlusSHOES+'Pantheon_PS1MD/',
        'snls': PPlusSHOES+'JLA2014_SNLS_DS17/',
        'swift_sne_ia': PPlusSHOES+'SWIFT/'
}
dir_name = {
        'cfa_snII': 'CFA_SNII',
        'csp_dr3': 'CSPDR3',
        'des_y3_sne_ia': 'des_y3_sne_ia',
        'foundation': 'foundation_dr1',
        'ps1_sne_ia': 'ps1_sne_ia',
        'snls': 'snls',
        'swift_sne_ia': 'swift_sne_ia',
}
survey_specific_logic = {
        'cfa_snII': cfa_snII_logic,
        'csp_dr3': csp_dr3_logic,
}
for dataset in SNANA_DATASETS:
    survey_specific_logic[dataset] = github_logic

def main(args):
    file_name = file_names[args.dataset]
    # Construct the URL to download the file from Zenodo
    url = urls[args.dataset]+file_name

    os.makedirs(os.path.join(args.destination_path, dir_name[args.dataset]), exist_ok=True)
    survey_specific_logic[args.dataset](args)
    # Print a success message if the file is downloaded successfully
    print(f"File downloaded successfully to {args.destination_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Transfer data to user-provided destination folder."
    )
    parser.add_argument(
        "destination_path",
        type=str,
        help="The destination path to download and unzip the data into.",
    )
    parser.add_argument(
        "dataset",
        type=str,
        help="The dataset to be downloaded",
    )
    args = parser.parse_args()
    try:
        urls[args.dataset]
    except KeyError:
        raise KeyError(f'Dataset argument should be in {urls.keys()}')
    main(args)

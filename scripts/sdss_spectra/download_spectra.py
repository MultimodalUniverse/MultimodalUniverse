import os
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import multiprocessing
import spender
from spender.data.sdss import SDSS
from spender.instrument import get_skyline_mask
import torch
import h5py
import re
import astropy.io.fits as fits
import numpy as np
import matplotlib.pyplot as plt

data_path = "/mnt/home/lparker/ceph/sdss/"
save_path = "/mnt/home/lparker/ceph/sdss_spectra/"
wave_obs = 10 ** torch.arange(3.578, 3.97, 0.0001)
dataset_names = ['target_id', 'obj_ra', 'obj_dec', 'z', 'zerr', 'spec', 'ivar']

def list_subdirectories_in_directory(directory_url):
    try:
        response = requests.get(directory_url)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            links = soup.find_all('a')
            subdirectories = [link.get('href') for link in links if link.get('href') and link.get('href').endswith('/')]
            subdirectories = [os.path.basename(d.rstrip('/')) for d in subdirectories if d[0].isdigit()]
            return subdirectories
        else:
            print(f"Failed to retrieve directory: {response.status_code} - {response.reason}")
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        
def list_files_in_directory(directory_url):
    try:
        response = requests.get(directory_url)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            links = soup.find_all('a')
            file_names = [link.get('href') for link in links if link.get('href') is not None and link.get('href').startswith('spec')]
            return file_names
        else:
            print(f"Failed to retrieve directory: {response.status_code} - {response.reason}")
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        
def download_file(filename, dir):
    match = re.search(r'-(\d+)-', filename)
    plate = match.group(1)
    dirname = os.path.join(dir, str(plate).zfill(4))
    flocal = os.path.join(dirname, filename)

    if not os.path.isfile(flocal):
        os.makedirs(dirname, exist_ok=True)
        url = "%s/%s/%s" % (_base_url, str(plate).zfill(4), filename)
        urllib.request.urlretrieve(url, flocal)
        
data_dir = "https://data.sdss.org/sas/dr16/sdss/spectro/redux/26/spectra/lite/"
subdirectories = list_subdirectories_in_directory(data_dir)

dir = '/mnt/home/lparker/ceph/sdss/'

progress_bar = tqdm(total=len(subdirectories), desc = 'Downloading Spectra')

for sub in subdirectories:
    url = os.path.join(data_dir, sub)
    file_names = list_files_in_directory(url)
    num_cores = multiprocessing.cpu_count()
    pool = multiprocessing.Pool(processes=num_cores)
    pool.starmap(download_file, [(filename, dir) for filename in file_names])
    pool.close()
    pool.join()   
    progress_bar.update(1)
    
progress_bar.close()
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
from astropy.table import Table

data_path = "/mnt/home/lparker/ceph/sdss/"
save_path = "/mnt/home/lparker/ceph/sdss_spectra/"
wave_obs = 10 ** torch.arange(3.578, 3.97, 0.0001)
dataset_names = ['target_id', 'obj_ra', 'obj_dec', 'z', 'zerr', 'spec', 'ivar']

def process_spectrum(filename):
    file = os.path.basename(filename)
    plate, mjd, fiberid = re.findall(r'\d+', file)
    hdulist = fits.open(filename)
    
    data = hdulist[1].data
    specinfo = hdulist[2].data
    
    info = {}
    
    # Initial Info
    target_id = specinfo['OBJID'][0]    
    if target_id == '': info['target_id'] = -1
    else: info['target_id'] = int(target_id)
    info['obj_ra'] = specinfo['PLUG_RA']
    info['obj_dec'] = specinfo['PLUG_DEC']
    info['z'] = specinfo['Z']
    info['zerr'] = specinfo['Z_ERR']
    
    # Merge spectrum and ivar into wave_obs length 
    loglam = data['loglam']
    flux = data['flux']
    ivar = data['ivar']
    
    # Apply bitmask for instrumentation malfunction
    mask = data['and_mask'].astype(bool) | (ivar <= 1e-6)
    ivar[mask] = 0
    
    L = len(wave_obs)
    start = int(np.around((loglam[0] - torch.log10(wave_obs[0]).item())/0.0001))
    if start<0:
        flux = flux[-start:]
        ivar = ivar[-start:]
        end = min(start+len(loglam), L)
        start = 0
    else:
        end = min(start+len(loglam), L)
    spec = np.zeros(L)
    w = np.zeros(L)
    
    spec[start:end]  = flux
    w[start:end] = ivar

    info['spec'] = spec
    info['ivar'] = w
    
    selected_keys = ['target_id', 'obj_ra', 'obj_dec']
    table_info = {key: info[key] for key in selected_keys}
    
    return (info, table_info)

def process_single_file(filename):
    info = process_spectrum(filename)
    return info

if __name__ == '__main__':
    batch_size = 200000
    k = 0
    num_cores = multiprocessing.cpu_count()
    print(f'Using {num_cores} multi-processing cores...')

    print('------ Starting File Grab ------')
    all_files = []
    with tqdm(total = len(os.listdir(data_path)), desc='Grabbing Files') as pbar:
        for root, _, files in os.walk(data_path):
            for file_name in files:
                file_path = os.path.join(root, file_name)
                all_files.append(file_path)
            pbar.update(1)

    print('------ Starting processing ------')
    for i in range(0, len(all_files), batch_size):
        print(f'Processing files {i} to {i+batch_size}')
        batch = all_files[i:i + batch_size]
        filename = save_path + str(k) + '.h5'

        with h5py.File(filename, 'w') as f:
            for name in dataset_names:
                if name == 'spec' or name == 'ivar':
                    f.create_dataset(name, (len(batch), len(wave_obs)), dtype='float32')
                elif name == 'target_id':
                    f.create_dataset(name, len(batch), dtype='float64')
                else:
                    f.create_dataset(name, len(batch), dtype='float32') 

        args_list = [filename for filename in batch]
        with multiprocessing.Pool(processes=num_cores) as pool:
            info_list = list(tqdm(pool.imap(process_single_file, args_list), total=len(batch), desc='Getting info list'))

        selected_keys = ['target_id', 'obj_ra', 'obj_dec', 'h5 file', 'index']
        table = Table(names = selected_keys)

        with h5py.File(filename, 'a') as f:
            with tqdm(total=len(batch), desc='Saving files') as pbar:
                for j, info in enumerate(info_list):
                    for name in dataset_names:
                        f[name][j] = info[0][name]

                    astrotable_info = info[1]
                    astrotable_info['h5 file'] = i
                    astrotable_info['index'] = j

                    table.add_row(astrotable_info)
                    pbar.update(1)

        table.write(save_path + f'{k}.dat', format='ascii')    
        k += 1
        
    print('------ Done processing! ------')
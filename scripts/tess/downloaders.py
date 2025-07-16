_healpix_nside = 16
_TINY_SIZE = 100 # Number of light curves to use for testing.
_BATCH_SIZE = 100 # number of light curves requests to submit to MAST at a time. These are processed in parallel.
PAUSE_TIME = 3 # Pause time between retries to MAST server
_CHUNK_SIZE = 8192

import shutil
import os 
from astropy.io import fits
import numpy as np
from astropy.table import Table, join, Row
import h5py
import healpy as hp
from multiprocessing import Pool
from tqdm import tqdm
import subprocess
from abc import ABC, abstractmethod
import time
import aiohttp
import asyncio


# In the inherited class - implement the correct data cleaning procedures.

class TESS_Downloader(ABC):
    '''
    A helper class for downloading and processing TESS lightcurve data from various pipelines.

    Figure out what can be kept in this function as general and what needs to added to the child 
    classes. 

    Attributes
    ----------
    sector: int, 
        The TESS sector number.
    sector_str: str, 
        The TESS sector string.
    pipeline: str, 
        The TESS pipeline to use.
    data_path: str, 
        Path to the directory containing the pipeline data catalogues and .sh file.
    hdf5_output_dir: str, 
        Path to the directory to save the hdf5 files.
    fits_dir: str, 
        Path to the directory to save the fits files.
    n_processes: int, 
        Number of processes to use for parallel processing.
    async_downloads: bool,
        Whether the MAST should be queried asynchronously.

    Methods
    -------
    read_sh(fp: str)
        Read an sh file and return the curl commands
    parse_line(line: str)
        Parse a line from the sh file and return the relevant parameters
    parse_curl_commands(sh_file)
        Parse the curl commands from the sh file
    create_sector_catalog(save_catalog: bool = False, tiny: bool = True)
        Create a sector catalog from the .sh file
    get_fits_lightcurve(catalog_row)
        Download the light curve file using the curl command and save it to the output file
    processing_fn(row)
        Process a single light curve file into the standard format
    save_in_standard_format(catalog, filename)
        Save the standardised batch of light curves dict in a hdf5 file
    '''

    def __init__(self, sector: int, data_path: str, hdf5_output_dir: str, fits_dir: str, n_processes: int = 1, async_downloads: bool = True):   
        '''
        Initialisation for the TESS_Downloader class
        Parameters
        ----------
        sector: int, 
            The TESS sector number.
        pipeline: str, 
            The TESS pipeline to use.
        data_path: str, 
            Path to the directory containing the pipeline data.
        hdf5_output_dir: str, 
            Path to the directory to save the hdf5 files
        fits_dir: str, 
            Path to the directory to save the fits files
        n_processes: int, 
            Number of processes to use for parallel processing
        async_downloads: bool,
            Whether the MAST should be queried asynchronously

        Returns
        -------
        None
        '''
        self.sector = sector
        self.sector_str = f's{sector:04d}'
        self.data_path = data_path
        self.hdf5_output_dir = hdf5_output_dir
        self.fits_dir = fits_dir
        self.n_processes = n_processes
        self.async_downloads = async_downloads

    def __repr__(self) -> str:
        return f"TESS_Downloader(sector={self.sector}, data_path={self.data_path}, hdf5_output_dir={self.hdf5_output_dir}, fits_dir={self.fits_dir}, n_processes={self.n_processes})"

    @staticmethod
    def read_sh(fp: str) -> list[str]:
        '''
        read_sh reads an sh file, parses and returns the curls commands from the file
        
        Parameters
        ----------
        fp: str, path to the .sh file
        
        Returns
        ------- 
        lines: list, list of curl commands in the .sh file for downloading a single light curve
        '''

        with open(fp, 'r') as f:
            lines = f.readlines()[1:]
        return lines

    def parse_curl_commands(self, sh_fp: str) -> list[list[int]]:
        '''
        Parse the curl commands from the .sh file

        Parameters
        ----------
        sh_file: str, path to the .sh file
        
        Returns
        ------- 
        params: list, list of parameters extracted from the lines
        '''

        lines = self.read_sh(sh_fp)
        params = list(self.parse_line(line) for line in lines)
        return params 
    
    @abstractmethod
    def parse_line(self, line: str) -> list[int]:
       pass
    
    @abstractmethod
    def fits_url(self):
        pass 
    
    def get_fits_lightcurve(self, catalog_row: Row) -> bool: # catalog_row : type
        '''
        Download the light curve file using the curl command and save it to the output file

        Parameters
        ----------
        curl_cmd: str, curl command
        output_fp: str, path to the output file
        
        Returns
        ------- 
        success: bool, True if the download was successful, False otherwise
        '''

        url, path = self.fits_url(catalog_row)
        cmd = f'curl {url} --create-dirs -o {os.path.join(self.fits_dir, path)}'

        try:
            subprocess.run(cmd, shell=True, check=True, text=True, capture_output=True)
            return True #, f"Successfully downloaded: {output_fp}"
        
        except subprocess.CalledProcessError:
            return False #, f"Error downloading using the following cmd: {cmd}: {e.stderr}"
        
    @abstractmethod
    def processing_fn(
            self,
            del_fits: bool = True
    ):
        pass

    def save_in_standard_format(self, args: tuple[Table, str], del_fits: bool = True) -> bool:
        '''
        Save the standardised batch of light curves dict in a hdf5 file 

        Parameters
        ----------
        args: tuple, tuple of arguments: (subcatalog, output_filename)

        Returns
        -------
        success: bool, True if the file was saved successfully, False otherwise
        '''

        subcatalog, output_filename = args
        
        if not os.path.exists(os.path.dirname(output_filename)):
            os.makedirs(os.path.dirname(output_filename))

        results = [] 
        for row in tqdm(subcatalog):
            result = self.processing_fn(row, del_fits=del_fits)
            if result is not None: # Usually for files not found.
                results.append(result)

        max_length = max([len(d['time']) for d in results])

        for i in range(len(results)):
            for key in results[i].keys():
                if isinstance(results[i][key], np.ndarray):
                    results[i][key] = np.pad(results[i][key], (0,max_length - len(results[i][key])), mode='constant')

        lightcurves = Table({k: [d[k] for d in results]
                        for k in results[0].keys()})
        lightcurves.convert_unicode_to_bytestring()

        with h5py.File(output_filename, 'w') as hdf5_file:
            for key in lightcurves.colnames:
                hdf5_file.create_dataset(key, data=lightcurves[key])
        return 1

    @abstractmethod
    def download_sh_script(self, show_progress: bool = False) -> bool:
       pass
    
    @property
    @abstractmethod
    def csv_url(self):
        pass

    @csv_url.setter
    def csv_url(self): 
        pass

    @property
    @abstractmethod
    def sh_url(self):
        pass

    @sh_url.setter
    def sh_url(self): 
        pass
    
    @property
    @abstractmethod
    def catalog_column_names(self):
        pass

    @catalog_column_names.setter
    def catalog_column_names(self):
        pass

    def download_target_csv_file(self, show_progress: bool = False) -> bool:
        '''
        Download the target list csv file from the QLP MAST site (https://archive.stsci.edu/hlsp/qlp)

        Parameters
        ----------
        output_path: str, path to the output directory
        show_progress: bool, if True, show the progress of the download

        Returns
        -------
        success: bool, True if the file was downloaded successfully, False otherwise

        '''

        try:    
            output_file = os.path.join(self.data_path, f"{self.sector_str}_target_list.csv")
            if os.path.exists(output_file):
                print(f"File already exists at {output_file}, skipping download.")
                return True
            
            os.makedirs(self.data_path, exist_ok=True)
            fp = os.path.join(self.data_path, f"{self.sector_str}_target_list.csv")
                        
            curl_cmd = f'wget {"--progress=bar:force --show-progress" if show_progress else ""} {self.csv_url} -O {fp}'
            
            if show_progress:
                result = subprocess.run(curl_cmd, shell=True, check=True, text=True)
            else:
                result = subprocess.run(curl_cmd, shell=True, check=True, text=True, capture_output=True)

            if result.returncode == 0:
                print(f"Successfully downloaded: {fp}")
                return True
            else:
                print(f"Error downloading file: {result.stderr}")
                return False
            
        except Exception as e:
            print(f"Error downloading csv file from {self.csv_url}: {e}")
            return False

    def download_sh_script(self, show_progress: bool = False) -> bool:
        '''
        Download the sh script from the QLP MAST site ()

        Parameters
        ----------
        output_path: str, path to the output directory
        show_progress: bool, if True, show the progress of the download

        Returns
        -------
        success: bool, True if the file was downloaded successfully, False otherwise

        Raises
        ------
        Exception: if there is an error downloading the file
        ''' 
        
        try:
            # Check if file already exists
            output_file = os.path.join(self.data_path, f"{self.sector_str}_fits_download_script.sh")
            if os.path.exists(output_file):
                print(f"File already exists at {output_file}, skipping download.")
                return True
            
            os.makedirs(self.data_path, exist_ok=True)

            curl_cmd = f'wget {"--progress=bar:force --show-progress" if show_progress else ""} {self.sh_url} -O {os.path.join(self.data_path, f"{self.sector_str}_fits_download_script.sh")}'
            
            if show_progress:
                result = subprocess.run(curl_cmd, shell=True, check=True, text=True)
            else:
                result = subprocess.run(curl_cmd, shell=True, check=True, text=True, capture_output=True)

            if result.returncode == 0:
                print(f"Successfully downloaded: {self.data_path}/{self.sector_str}_fits_download_script.sh")
                return True
            else:
                print(f"Error downloading file: {result.stderr}")
                return False
            
        except Exception as e:
            print(f"Error downloading .sh file from {self.sh_url}: {e}")
            return False
    
    async def download_fits_file(self, session, url, output_path, max_retries=3, base_delay=2):
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        for attempt in range(max_retries):
            try:
                async with session.get(url) as response:
                    if response.status == 200:
                        with open(output_path, 'wb') as f:
                            async for chunk in response.content.iter_chunked(_CHUNK_SIZE):  # 8KB chunks
                                f.write(chunk)
                        return output_path
                    elif response.status == 429:
                        delay = base_delay * 2**attempt # exponential backoff for finding required delay
                        print(f"Rate limited. Waiting {delay} seconds...")
                        await asyncio.sleep(delay)
                        continue
                    else:
                        print(f"Failed to download {url}: Status {response.status}")
                        if attempt < max_retries - 1:
                            delay = base_delay * (2 ** attempt)
                            print(f"Retrying in {delay} seconds... (Attempt {attempt + 1}/{max_retries})")
                            await asyncio.sleep(delay)
                            continue

            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                if attempt < max_retries - 1:
                    delay = base_delay * (2 ** attempt)
                    print(f"Error: {str(e)}. Retrying in {delay} seconds... (Attempt {attempt + 1}/{max_retries})")
                    await asyncio.sleep(delay)
                else:
                    print(f"Failed to download {url} after {max_retries} attempts: {str(e)}")
                    return False
    
    async def download_batch(self, catalog_batch: Table) -> list[bool]:
        if not os.path.exists(self.fits_dir):
            os.makedirs(self.fits_dir, exist_ok=True)

        urls, paths = zip(*[self.fits_url(row) for row in catalog_batch])
        #fits_paths = [self.fits_url(row) for row in catalog_batch]

        async with aiohttp.ClientSession() as session:
            tasks = []
            for i, url in enumerate(urls):
                task = asyncio.create_task(self.download_fits_file(session, url, os.path.join(self.fits_dir, paths[i])))
                tasks.append(task)
            
            completed_files = await asyncio.gather(*tasks)
            print(f"Downloaded {len(completed_files)} files")
        return completed_files
    
    def download_sector_catalog_lightcurves(
            self,
            catalog: Table # batch of light curves to get from MAST,
        ) -> list[bool]:
        '''
        Download the light curves for a given sector and save them in the standard format

        Parameters
        ----------
        catalog: Table,
        tiny: bool, if True, only use a small sample of 100 objects for testing

        Returns
        -------
        results: list, list of tuples containing the success status and the message for each light curve download
        '''
        if not os.path.exists(self.fits_dir):
            os.makedirs(self.fits_dir, exist_ok=True)
        if self.async_downloads:
            results = asyncio.run(self.download_batch(catalog))
        else:
            with Pool(self.n_processes) as pool:
                results = list(tqdm(pool.imap(self.get_fits_lightcurve, [row for row in catalog]), total=len(catalog)))
                
        '''if sum([result for result in results]) != len(catalog):
            print("There was an error in the parallel processing of the download of the fits files, some files may not have been downloaded.")'''
        return results
    
    @property
    @abstractmethod
    def pipeline(self):
        pass

    @pipeline.setter
    def pipeline(self): # Check pipeline here. Is this needed?
        pass
    
    def create_sector_catalog(self, save_catalog: bool = False, tiny: bool = True) -> Table:
        '''
        Create a sector catalog from the .sh file. Sector catalogs contains: gaiadr3_id, cam, ccd, fp1, fp2, fp3, fp4 
        for each light curve in the sector.

        Parameters
        ----------
        tiny: bool, if True, only use a small sample of 100 objects for testing
`
        Returns
        ------- 
        catalog: astropy Table, sector catalog
        '''

        self.sh_fp = os.path.join(self.data_path, f'{self.sector_str}_fits_download_script.sh')
        params = self.parse_curl_commands(self.sh_fp)
        
        catalog = Table(rows=params, names=self.catalog_column_names)

        if tiny:
            catalog = catalog[0:_TINY_SIZE]

        # Merge with target list to get RA-DEC
        self.csv_fp = os.path.join(self.data_path, f'{self.sector_str}_target_list.csv')
        target_list = Table.read(self.csv_fp, format='csv')
        target_list.rename_column('#' + self.catalog_column_names[0], self.catalog_column_names[0]) 

        catalog = join(catalog, target_list, keys=self.catalog_column_names[0], join_type='inner') # remove duplicates from qlp

        catalog['healpix'] = hp.ang2pix(_healpix_nside, catalog['RA'], catalog['DEC'], lonlat=True, nest=True)

        if save_catalog:
            self.catalog_fp = os.path.join(self.data_path, f'{self.sector_str}_catalog{"_tiny" if tiny else ""}.hdf5')
            catalog.write(self.catalog_fp , format='hdf5', overwrite=True, path=self.catalog_fp )
            print(f"Saved catalog to {self.catalog_fp }")
        return catalog
        
    def convert_fits_to_standard_format(self, catalog: Table) -> list[bool]:
        '''
        Convert the fits light curves to the standard format and save them in a hdf5 file

        Parameters
        ----------
        catalog: astropy.Table, sector catalog

        Returns
        -------
        results: list, list of booleans indicating the success of the conversion for each light curve
        '''

        catalog = catalog.group_by(['healpix'])

        map_args = []
        for group in catalog.groups: 
            group_filename = os.path.join(self.hdf5_output_dir, '{}/healpix={}/001-of-001.hdf5'.format(self.pipeline, group['healpix'][0]))
            map_args.append((group, group_filename))

        with Pool(self.n_processes) as pool:
            results = list(tqdm(pool.imap(self.save_in_standard_format, map_args), total=len(map_args)))

        if sum(results) != len(map_args):
            print("There was an error in the parallel processing of the fits files to standard format, some files may not have been processed correctly")
        return results
    
    def batcher(self, seq: list, batch_size: int) -> list[list]:
        return (seq[pos:pos + batch_size] for pos in range(0, len(seq), batch_size))

    def batched_download(self, catalog: Table, tiny: bool) -> list[list[bool]]:
        if tiny:
            results = self.download_sector_catalog_lightcurves(catalog=catalog[:_TINY_SIZE])
        else:
            catalog_len = len(catalog)
            results = []
            for batch in tqdm(self.batcher(catalog, _BATCH_SIZE), total = catalog_len // _BATCH_SIZE):
                try:
                    results.append(self.download_sector_catalog_lightcurves(batch))
                except Exception as e:
                    print(f"Error downloading light curves: {e}. Waiting {PAUSE_TIME} seconds before retrying...")
                    time.sleep(PAUSE_TIME)
                    results.append(self.download_sector_catalog_lightcurves(batch))

        return results
        
    def download_sector(
            self,
            tiny: bool = True, 
            show_progress: bool = False,
            save_catalog: bool = True,
            clean_up: bool = True
    ) -> bool:
        '''
        Download the sector data from the QLP-MAST site and save it in the standard format 

        Parameters
        ----------
        tiny: bool, if True, only use a small sample of 100 objects for testing
        show_progress: bool, if True, show the progress of the download

        Returns
        -------
        success: bool, True if the download was successful, False otherwise
        '''
    
        # Download the sh file from the QLP site
        self.download_sh_script(show_progress) 

        # Download the target list csv file from the QLP site
        self.download_target_csv_file(show_progress)

        # Create the sector catalog
        catalog = self.create_sector_catalog(save_catalog = save_catalog, tiny = tiny)
        # Download the fits light curves using the sector catalog

        if self.async_downloads:
            self.batched_download(catalog, tiny)

        else:
            self.batched_download(catalog, tiny) # To-DO: You can use the results to check if the download was successful

        # Process fits to standard format
        if tiny:
            self.convert_fits_to_standard_format(catalog[:_TINY_SIZE])
        else:
            self.convert_fits_to_standard_format(catalog)

        # TO-DECIDE: clean-up of fits, .sh and .csv files
        if clean_up:
            self.clean_up()

        return 1
    
    def clean_up(self) -> None:
        '''
        Clean-up for the fits, .sh and .csv files to free up disk space after the parent sample has been built.

        Parameters
        ----------
        fits_dir: str, path to the fits directory
        sh_dir: str, path to the sh directory
        csv_dir: str, path to the csv directory

        Returns
        -------
        success: bool, True if the clean-up was successful, False otherwise
        '''
        for fp in [self.sh_fp, self.csv_fp, self.catalog_fp]:
            os.remove(fp)

        if self.fits_dir is not None:
            print(f"Cleaning up fits directory: {self.fits_dir}")  
            shutil.rmtree(self.fits_dir, ignore_errors=True)

        return


class SPOC_Downloader(TESS_Downloader):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.sector > 72:
            raise ValueError(f"SPOC does not have data for sector {self.sector}. Please use a sector in the range 1-80.")
        
        self._pipeline = 'SPOC'
        self.fits_base_url = f'https://mast.stsci.edu/api/v0.1/Download/file/?uri=mast:HLSP/tess-spoc/{self.sector_str}/target/'
        self._sh_base_url = f'https://archive.stsci.edu/hlsps/tess-spoc/download_scripts/hlsp_tess-spoc_tess_phot_{self.sector_str}_tess_v1_dl-lc.sh'
        self._csv_base_url = f'https://archive.stsci.edu/hlsps/tess-spoc/target_lists/{self.sector_str}.csv'
        self._catalog_column_names = ['TIC_ID', 'fp1', 'fp2', 'fp3', 'fp4'] # put the joining id first!

    @property
    def pipeline(self): # TESS-Pipeline
        return self._pipeline
    
    @property
    def csv_url(self): 
        return self._csv_base_url
    
    @property
    def sh_url(self):
        return self._sh_base_url

    @property
    def catalog_column_names(self):
        return self._catalog_column_names
    
    # Add methods - processing_fn, fits_url, parse_line
    def parse_line(self, line: str) -> list[int]:
        '''
        Parse a line from the .sh file, extract the relevant parts (gaia_id, cam, ccd, fp1, fp2, fp3, fp4) and return them as a list

        Parameters
        ----------
        line: str, a line from the .sh file
        
        Returns
        ------- 
        params: list, list of parameters extracted from the line
        '''

        parts = line.split()
        output_path = parts[6].strip("'")
        path_parts = output_path.split('/')
        numbers = path_parts[2:6]

        filename = path_parts[-1]
        tic_id = filename.split('_')[4].split('-')[0]

        return [int(tic_id), *numbers]
    
    def fits_url(self, catalog_row: Row) -> tuple[str, str]:
        
        path = f'{catalog_row["fp1"]}/{catalog_row["fp2"]}/{catalog_row["fp3"]}/{catalog_row["fp4"]}/hlsp_tess-spoc_tess_phot_{catalog_row["TIC_ID"]:016d}-{self.sector_str}_tess_v1_lc.fits'
        url =  self.fits_base_url + path
        return url, path
    
    def processing_fn(
            self,
            catalog_row : Row,
            del_fits : bool = True
    ) -> dict:
        ''' 
        Process a single light curve file into the standard format 

        Parameters
        ----------
        row: astropy Row, a single row from the sector catalog containing the object descriptors: gaiadr3_id, cam, ccd, fp1, fp2, fp3, fp4
        
        Returns
        ------- 
        lightcurve: dict, light curve in the standard format
        i.e. 
            {
                'TIC_ID': tess id,  
                'time': obs_times: arr_like,
                'flux': fluxes: arr_like,
                'flux_err': psf_flux_err: float,
                'quality_flags': tess_flags: arr_like,
                'RA': ra: float,
                'DEC': dec # More columns maybe required...
            }
        '''

        fits_fp = os.path.join(self.fits_dir, self.fits_url(catalog_row)[1])
        try:
            with fits.open(fits_fp, mode='readonly', memmap=True) as hdu:
                entry = {
                    'object_id': catalog_row['TIC_ID'],
                    'time': hdu['LIGHTCURVE'].data['TIME'],
                    'flux': hdu['LIGHTCURVE'].data['SAP_FLUX'], #Note: PDCSAP_FLUX is processed.
                    'flux_err':  hdu['LIGHTCURVE'].data['SAP_FLUX_ERR'],
                    'quality': np.asarray(hdu['LIGHTCURVE'].data['QUALITY'], dtype='int32'),
                    'RA': hdu[1].header['ra_obj'],
                    'DEC': hdu[1].header['dec_obj']
                }
                if del_fits:
                    os.remove(fits_fp)
                    try:
                        os.rmdir(os.path.dirname(fits_fp))
                    except:
                        pass
                return entry

        except FileNotFoundError:
            print(f"File not found: {fits_fp}")
            return None


class TGLC_Downloader(TESS_Downloader):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if self.sector > 39:
            raise ValueError(f"TGLC is currently only available for sectors 1-39.") 

        self._pipeline = 'TGLC'
        self.fits_base_url =  f'https://archive.stsci.edu/hlsps/tglc/{self.sector_str}/'         
        self._sh_base_url = f'https://archive.stsci.edu/hlsps/tglc/download_scripts/hlsp_tglc_tess_ffi_{self.sector_str}_tess_v1_llc.sh'
        self._csv_base_url = f'https://archive.stsci.edu/hlsps/tglc/target_lists/{self.sector_str}.csv'
        self._catalog_column_names = ['GAIADR3_ID', 'cam', 'ccd', 'fp1', 'fp2', 'fp3', 'fp4'] # put the joining id first!
        
    @property
    def pipeline(self): # TESS-Pipeline
        return self._pipeline
    
    @property
    def csv_url(self): 
        return self._csv_base_url
    
    @property
    def sh_url(self):
        return self._sh_base_url

    @property
    def catalog_column_names(self):
        return self._catalog_column_names
    
    # Add methods - processing_fn, fits_url, parse_line
    def parse_line(self, line: str) -> list[int]:
        '''
        Parse a line from the .sh file, extract the relevant parts (tic_id, cam, ccd, fp1, fp2, fp3, fp4) and return them as a list

        Parameters
        ----------
        line: str, a line from the .sh file
        
        Returns
        ------- 
        params: list, list of parameters extracted from the line
        '''
        parts = line.split()
        output_path = parts[4].strip("'")
            
        # Split the path and extract the relevant parts
        path_parts = output_path.split('/')
        
        # Get TIC_ID from filename    
        cam = int(path_parts[1].split('-')[0].split('cam')[1])
        ccd = int(path_parts[1].split('-')[1].split('ccd')[1])
        numbers = path_parts[2:6]
        gaia_id = path_parts[-1].split('-')[1]

        
        return [int(gaia_id), cam, ccd, *numbers]

    def fits_url(self, catalog_row: Row) -> tuple[str, str]:
        path = f'cam{catalog_row["cam"]}-ccd{catalog_row["ccd"]}/{catalog_row["fp1"]}/{catalog_row["fp2"]}/{catalog_row["fp3"]}/{catalog_row["fp4"]}/hlsp_tglc_tess_ffi_gaiaid-{catalog_row["GAIADR3_ID"]}-{self.sector_str}-cam{catalog_row["cam"]}-ccd{catalog_row["ccd"]}_tess_v1_llc.fits'
        url =  self.fits_base_url + path
        return url, path
    
    def processing_fn(
            self,
            catalog_row : Row,
            del_fits : bool = True
    ) -> dict:
        ''' 
        Process a single light curve file into the standard format 

        Parameters
        ----------
        row: astropy Row, a single row from the sector catalog containing the object descriptors: gaiadr3_id, cam, ccd, fp1, fp2, fp3, fp4
        
        Returns
        ------- 
        lightcurve: dict, light curve in the standard format
        i.e. 
            {
                'TIC_ID': tess id,
                'gaiadr3_id': gaia id,
                'time': obs_times: arr_like,
                'psf_flux': psf_fluxes: arr_like,
                'psf_flux_err': psf_flux_err: float,
                'aper_flux': aperture_fluxes: arr_like,
                'aper_flux_err': aperture_flux_err: float,
                'tess_flags': tess_flags: arr_like,
                'tglc_flags': tglc_flags: arr_like,
                'RA': ra: float,
                'DEC': dec # More columns maybe required...
            }
        '''
        fits_fp = os.path.join(self.fits_dir, self.fits_url(catalog_row)[1])

        try:
            with fits.open(fits_fp, mode='readonly', memmap=True) as hdu:
                entry = {
                    'object_id': catalog_row['TIC_ID'],
                    'GAIADR3_ID': catalog_row['GAIADR3_ID'],
                    'time': hdu[1].data['time'],
                    'psf_flux': hdu[1].data['psf_flux'],
                    'psf_flux_err': hdu[1].header['psf_err'],
                    'aper_flux': hdu[1].data['aperture_flux'],
                    'aper_flux_err': hdu[1].header['aper_err'],
                    'tess_flags': hdu[1].data['TESS_flags'],
                    'tglc_flags': hdu[1].data['TGLC_flags'],
                    'RA': hdu[1].header['ra_obj'],
                    'DEC': hdu[1].header['dec_obj']
                }
                if del_fits:
                    os.remove(fits_fp)
                    try:
                        os.rmdir(os.path.dirname(fits_fp))
                    except:
                        pass
                return entry
            
        except FileNotFoundError:
            print(f"File not found: {fits_fp}")
            return None

class QLP_Downloader(TESS_Downloader):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        if self.sector > 80:
            raise ValueError(f"QLP does not have data for sector {self.sector}. Please use a sector in the range 1-80.")
        
        self._pipeline = 'QLP'
        self.fits_base_url = f'https://mast.stsci.edu/api/v0.1/Download/file/?uri=mast:HLSP/qlp/{self.sector_str}/'
        self._sh_base_url = f'https://archive.stsci.edu/hlsps/qlp/download_scripts/hlsp_qlp_tess_ffi_{self.sector_str}_tess_v01_llc-fits.sh'
        self._csv_base_url = f'https://archive.stsci.edu/hlsps/qlp/target_lists/{self.sector_str}.csv'
        self._catalog_column_names = ['TIC_ID', 'fp1', 'fp2', 'fp3', 'fp4'] # put the joining id first!

    @property
    def pipeline(self): # TESS-Pipeline
        return self._pipeline
    
    @property
    def csv_url(self): 
        return self._csv_base_url
    
    @property
    def sh_url(self):
        return self._sh_base_url

    @property
    def catalog_column_names(self):
        return self._catalog_column_names
        
    def parse_line(self, line: str) -> list[int]:
        '''
        Parse a line from the .sh file, extract the relevant parts (gaia_id, cam, ccd, fp1, fp2, fp3, fp4) and return them as a list

        Parameters
        ----------
        line: str, a line from the .sh file
        
        Returns
        ------- 
        params: list, list of parameters extracted from the line
        '''
        parts = line.split()
        output_path = parts[4].strip("'")
            
        # Split the path and extract the relevant parts
        path_parts = output_path.split('/')
        numbers = path_parts[1:5]
        TIC_ID = path_parts[-1].split('-')[1].split('_')[0]

        return [int(TIC_ID), *numbers]
    
    def fits_url(self, catalog_row: Row) -> tuple[str, str]:
        path = f'{catalog_row["fp1"]}/{catalog_row["fp2"]}/{catalog_row["fp3"]}/{catalog_row["fp4"]}/hlsp_qlp_tess_ffi_{self.sector_str}-{catalog_row["TIC_ID"]:016d}_tess_v01_llc.fits'
        url =  self.fits_base_url + path
        return url, path
    
    def processing_fn(
            self,
            catalog_row : Row,
            del_fits : bool = True
    ) -> dict:
        ''' 
        Process a single light curve file into the standard format 

        Parameters
        ----------
        catalog_row: astropy Row, a single row from the sector catalog containing the object descriptors: gaiadr3_id, cam, ccd, fp1, fp2, fp3, fp4
        
        Returns
        ------- 
        lightcurve: dict, light curve in the standard format
        i.e. 
            {
                'TIC_ID': id: str,
                'time': times: arr_like,
                'sap_flux': simple aperture fluxes: arr_like,
                'kspsap_flux': KSP aperture fluxes: arr_like,
                'kspsap_flux_err': KSP aperture fluxes errors: arr_like,
                'quality': quality flags: arr_like,
                'orbitid': orbit id: arr_like,
                'sap_x': sap x positions: arr_like,
                'sap_y': sap y positions: arr_like,
                'sap_bkg': background fluxes: arr_like  ,
                'sap_bkg_err': background fluxes errors: arr_like,
                'kspsap_flux_sml': small KSP aperture fluxes: arr_like,
                'kspsap_flux_lag': lagged KSP aperture fluxes: arr_like,
                'RA': ra: float,
                'DEC': dec: float,
                'tess_mag': tess magnitude: float,
                'radius': stellar radius: float,
                'teff': stellar effective temperature: float,
                'logg': stellar logg: float,
                'mh': stellar metallicity: float
            }
        '''
        fits_fp = os.path.join(self.fits_dir, self.fits_url(catalog_row)[1])
        try:
            with fits.open(fits_fp, mode='readonly', memmap=True) as hdu:
                # see docs @ https://archive.stsci.edu/hlsps/qlp/hlsp_qlp_tess_ffi_all_tess_v1_data-prod-desc.pdf
                entry = {
                    'object_id': catalog_row['TIC_ID'],
                    'time': hdu[1].data['time'],
                    'sap_flux': hdu[1].data['sap_flux'],
                    'kspsap_flux': hdu[1].data['kspsap_flux'],
                    'kspsap_flux_err': hdu[1].data['kspsap_flux_err'],
                    'quality': hdu[1].data['quality'],
                    'orbitid': hdu[1].data['orbitid'],
                    'sap_x': hdu[1].data['sap_x'],
                    'sap_y': hdu[1].data['sap_y'],
                    'sap_bkg': hdu[1].data['sap_bkg'],
                    'sap_bkg_err': hdu[1].data['sap_bkg_err'],
                    'kspsap_flux_sml': hdu[1].data['kspsap_flux_sml'],
                    'kspsap_flux_lag': hdu[1].data['kspsap_flux_lag'],
                    'RA': hdu[0].header['ra_obj'],
                    'DEC': hdu[0].header['dec_obj'],
                    'tess_mag': hdu[0].header['tessmag'],
                    'radius': hdu[0].header['radius'],
                    'teff': hdu[0].header['teff'],
                    'logg': hdu[0].header['logg'],
                    'mh': hdu[0].header['mh']
                }
                if del_fits:
                    os.remove(fits_fp)
                    try:
                        os.rmdir(os.path.dirname(fits_fp))
                    except:
                        pass
                return entry
            
        except FileNotFoundError:
            print(f"File not found: {fits_fp}")
            # Not sure why some files are not found in the tests
            return None

_CHUNK_SIZE = 8192
_healpix_nside = 16
_TINY_SIZE = 100 # Number of light curves to use for testing.
_BATCH_SIZE = 5000 # number of light curves requests to submit to MAST at a time. These are processed in parallel.
PAUSE_TIME = 3 # Pause time between retries to MAST server


import aiohttp
import asyncio
import time 
from tqdm.asyncio import tqdm_asyncio
import os 
from astropy.io import fits
import numpy as np
from astropy.table import Table, join, row
import h5py
import healpy as hp
from multiprocessing import Pool
from tqdm import tqdm
import subprocess
from abc import ABC, abstractmethod
import time


# TODO:
# Specify pipeline with pipeline flag: this will choose the URL for downloads
# In the inherited class - implement the correct data cleaning procedures.
# TESS-SPOC: https://archive.stsci.edu/hlsp/tess-spoc 

class TESS_Downloader(ABC):
    '''
    A helper class for downloading and processing TESS lightcurve data from various pipelines.

    Figure out what can be kept in this function as general and what needs to added to the child 
    classes. 

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

    Attributes
    ----------
    sector: int, 
        The TESS sector number.
    sector_str: str, 
        The TESS sector string.
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
    def __init__(self, sector: int, data_path: str, hdf5_output_dir: str, fits_dir: str, n_processes: int = 1, async_downloads: bool = False):    
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
    
    def get_fits_lightcurve(self, catalog_row: row.Row) -> bool: # catalog_row : type
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
            self
    ):
        pass

    def save_in_standard_format(self, args: tuple[Table, str]) -> bool:
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
            result = self.processing_fn(row)
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
        #url =  #f'https://archive.stsci.edu/hlsps/qlp/target_lists/{self.sector_str}.csv'

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
            
            if show_progress: # This could be cleaner
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
    
    async def download_fits_file(self, session, url, output_path):
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        async with session.get(url) as response:
            if response.status == 200:
                with open(output_path, 'wb') as f:
                    async for chunk in response.content.iter_chunked(_CHUNK_SIZE):  # 8KB chunks
                        f.write(chunk)
                return output_path
            else:
                print(f"Failed to download {url}: Status {response.status}")
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

        sh_fp = os.path.join(self.data_path, f'{self.sector_str}_fits_download_script.sh')
        params = self.parse_curl_commands(sh_fp)
        
        catalog = Table(rows=params, names=self.catalog_column_names)

        if tiny:
            catalog = catalog[0:_TINY_SIZE]

        # Merge with target list to get RA-DEC
        csv_fp = os.path.join(self.data_path, f'{self.sector_str}_target_list.csv')
        target_list = Table.read(csv_fp, format='csv')
        target_list.rename_column('#' + self.catalog_column_names[0], self.catalog_column_names[0]) 

        catalog = join(catalog, target_list, keys=self.catalog_column_names[0], join_type='inner') # remove duplicates from qlp

        catalog['healpix'] = hp.ang2pix(_healpix_nside, catalog['RA'], catalog['DEC'], lonlat=True, nest=True)

        if save_catalog:
            output_fp = os.path.join(self.data_path, f'{self.sector_str}_catalog{"_tiny" if tiny else ""}.hdf5')
            catalog.write(output_fp, format='hdf5', overwrite=True, path=output_fp)
            print(f"Saved catalog to {output_fp}")
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

        catalog = catalog.group_by(['healpix']) # will this handle millions of light curves? Or is more batching required?

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

                    # Might be a good idea to do processing and clean-up here. 
                except Exception as e:
                    print(f"Error downloading light curves: {e}. Waiting {PAUSE_TIME} seconds before retrying...")
                    time.sleep(PAUSE_TIME)
                    results.append(self.download_sector_catalog_lightcurves(batch))
                
            if sum([result for result in results]) != catalog_len:
                print(f"There was an error in the bulk download of the fits files, {sum([result for result in results])} / {catalog_len} files have been successfully downloaded.")
        return results
        
    def download_sector(
            self,
            tiny: bool = True, 
            show_progress: bool = False,
            save_catalog: bool = True
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
            #asyncio.run(self.batched_download(catalog, tiny))
            self.batched_download(catalog, tiny)

        else:
            self.batched_download(catalog, tiny) # To-DO: You can use the results to check if the download was successful

        n_files = 0
        for _, _, files in os.walk(self.fits_dir):
            n_files += len([f for f in files if f.endswith('.fits')])

        assert n_files == len(catalog), f"Expected {len(catalog)} .fits files in {self.fits_dir}, but found {n_files}"

        # Process fits to standard format
        if tiny:
            self.convert_fits_to_standard_format(catalog[:_TINY_SIZE])
        else:
            self.convert_fits_to_standard_format(catalog)

        # TO-DECIDE: clean-up of fits, .sh and .csv files

        return 1
    
    def clean_up(self, fits_dir: str = None, sh_dir: str = None, csv_dir: str = None) -> bool:
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
        if fits_dir is not None:
            pass
        if sh_dir is not None:

            pass
        if csv_dir is not None:
            pass
        return 1
    
import datasets
from datasets import Features, Value, Sequence
import numpy as np 
import itertools
import h5py
import os
from astropy.io import fits
from astropy.table.row import Row

# Some data cleaning is required here especially for the NaN values.

# TESS Sectors
PM = list(range(1, 26 + 1)) # Primary Mission
EM1 = list(range(27, 55 + 1)) # Extended Mission 1
EM2 = list(range(56, 96 + 1)) # Extended Mission 2

_CITATION = """\
@ARTICLE{2020RNAAS...4..204H,
       author = {{Huang}, Chelsea X. and {Vanderburg}, Andrew and {P{\'a}l}, Andras and {Sha}, Lizhou and {Yu}, Liang and {Fong}, Willie and {Fausnaugh}, Michael and {Shporer}, Avi and {Guerrero}, Natalia and {Vanderspek}, Roland and {Ricker}, George},
        title = "{Photometry of 10 Million Stars from the First Two Years of TESS Full Frame Images: Part I}",
      journal = {Research Notes of the American Astronomical Society},
     keywords = {Space observatories, Astronomy data analysis, 1543, 1858, Astrophysics - Earth and Planetary Astrophysics, Astrophysics - Instrumentation and Methods for Astrophysics, Astrophysics - Solar and Stellar Astrophysics},
         year = 2020,
        month = nov,
       volume = {4},
       number = {11},
          eid = {204},
        pages = {204},
          doi = {10.3847/2515-5172/abca2e},
archivePrefix = {arXiv},
       eprint = {2011.06459},
 primaryClass = {astro-ph.EP},
       adsurl = {https://ui.adsabs.harvard.edu/abs/2020RNAAS...4..204H},
      adsnote = {Provided by the SAO/NASA Astrophysics Data System}
}
"""

_DESCRIPTION = """
Quick look-up pipeline: Light curves from TESS FFI images.
"""
_HOMEPAGE = "https://archive.stsci.edu/hlsp/qlp"
_LICENSE = "CC BY 4.0"
_VERSION = "0.0.1"

class SPOC_Config(datasets.BuilderConfig):
    """BuilderConfig for QLP."""

    def __init__(self, data_files, **kwargs):
        """BuilderConfig for QLP.

        Parameters
        ----------

        Returns
        -------
        None
        """
        super().__init__(**kwargs)

        self.data_files = data_files
        self.citation = _CITATION
        self.homepage = _HOMEPAGE
        self.version = _VERSION
        self.license = _LICENSE


class SPOC(datasets.GeneratorBasedBuilder):
    VERSION = "0.0.1"
    DEFAULT_CONFIG_NAME = "all"

    BUILDER_CONFIG_CLASS = SPOC_Config
        
    @classmethod
    def _info(self):
        features = {"lightcurve" : Sequence({
            'time': Value(dtype="float32"),
            'flux': Value(dtype="float32"),
            'flux_err':  Value(dtype="float32"),
            'quality':  Value(dtype="float32")
            }),
            'RA':  Value(dtype="float32"),
            'DEC':  Value(dtype="float32"),
            'TIC_ID': Value(dtype="string")
        }
        
        return datasets.DatasetInfo(
            description=_DESCRIPTION,
            features = Features(features),
            homepage=_HOMEPAGE,
            license=_LICENSE,
            citation=_CITATION
        )

    def _split_generators(self, dl_manager):
        if not self.config.data_files:
            raise ValueError(
                f"At least one data file must be specified, but got data_files={self.config.data_files}"
            )
        data_files = dl_manager.download_and_extract(self.config.data_files)
        if isinstance(data_files, (str, list, tuple)):
            files = data_files
            if isinstance(files, str):
                files = [files]
            # Use `dl_manager.iter_files` to skip hidden files in an extracted archive
            files = [dl_manager.iter_files(file) for file in files]
            return [
                datasets.SplitGenerator(
                    name=datasets.Split.TRAIN, gen_kwargs={"files": files}
                )
            ]
        splits = []
        for split_name, files in data_files.items():
            if isinstance(files, str):
                files = [files]
            # Use `dl_manager.iter_files` to skip hidden files in an extracted archive
            files = [dl_manager.iter_files(file) for file in files]
            splits.append(
                datasets.SplitGenerator(name=split_name, gen_kwargs={"files": files})
            )
        return splits
    
    def _generate_examples(self, files, object_ids=None):
        """Yields examples as (key, example) tuples."""
        for j, file in enumerate(itertools.chain.from_iterable(files)):

            with h5py.File(file, "r") as data:
                if object_ids is not None:
                    keys = object_ids[j]
                else:
                    keys = data["TIC_ID"][:]
                
                # Preparing an index for fast searching through the catalog
                sort_index = np.argsort(data["TIC_ID"][:])
                sorted_ids = data["TIC_ID"][:][sort_index]
               
                for k in keys:
                    # Extract the indices of requested ids in the catalog
                    i = sort_index[np.searchsorted(sorted_ids, k)]
                   
                    example = {"lightcurve" : {
                        'time':  data["time"][i],
                        'flux': data["flux"][i],
                        'flux_err':  data["flux_err"][i],
                        'quality':  data["quality"][i]
                        },
                        'RA':  data["RA"][i],
                        'DEC':  data["DEC"][i],
                        'TIC_ID': data["TIC_ID"][i]
                    }

                    yield str(data["TIC_ID"][i]), example 

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
        path =  f'{catalog_row["fp1"]}/{catalog_row["fp2"]}/{catalog_row["fp3"]}/{catalog_row["fp4"]}/hlsp_tess-spoc_tess_phot_{f'{catalog_row["TIC_ID"]:016d}'}-{self.sector_str}_tess_v1_lc.fits'
        url =  self.fits_base_url + path
        return url, path
    
    def processing_fn(
            self,
            catalog_row : Row
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
        
                return {'TIC_ID': catalog_row['TIC_ID'],
                        'time': hdu['LIGHTCURVE'].data['TIME'],
                        'flux':  hdu['LIGHTCURVE'].data['PDCSAP_FLUX'],
                        'flux_err':  hdu['LIGHTCURVE'].data['PDCSAP_FLUX_ERR'],
                        'quality': np.asarray(hdu['LIGHTCURVE'].data['QUALITY'], dtype='int32'),
                        'RA': hdu[1].header['ra_obj'],
                        'DEC': hdu[1].header['dec_obj']
                        }

        except FileNotFoundError:
            print(f"File not found: {fits_fp}")
            return None
        # asyncio.run(main())

import time 

if __name__ == "__main__":
    start_time = time.time()

    spoc_downloader = SPOC_Downloader(
        sector = 64, 
        data_path = "./test", 
        hdf5_output_dir = "./test",
        fits_dir = "./test",
        n_processes = 6,
        async_downloads=True
    )
    tiny = True # Or False for a full sector download
    save_catalog = True # Flag whether to save the catalog to memory or not.

    spoc_downloader.download_sector()

    print(f"Time taken: {time.time() - start_time}")
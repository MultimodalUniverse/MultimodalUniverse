_healpix_nside = 16
_TINY_SIZE = 64 # Number of light curves to use for testing.
_BATCH_SIZE = 100 # number of light curves requests to submit to MAST at a time. These are processed in parallel.
PAUSE_TIME = 5 # Pause time between retries to MAST server
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
from database import DatabaseManager
import hashlib
import logging
from astroquery.mast import Observations, Catalogs
from astropy.table import vstack

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('tess_downloader')

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
    db_manager: DatabaseManager
        Manager for tracking download status and errors.

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

    def __init__(self, sector: int, data_path: str, hdf5_output_dir: str, fits_dir: str, n_processes: int = 1, async_downloads: bool = True, db_path: str = None):   
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
        db_path: str, optional
            Path to the database file for tracking downloads. If None, uses 'tess_downloads.db' in data_path.

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
        
        # Initialize database manager
        if db_path is None:
            db_path = os.path.join(data_path, 'tess_downloads.db')
        self.db_manager = DatabaseManager(db_path)
        logger.info(f"Initialized database at {db_path}")

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
    
    def get_fits_lightcurve(self, catalog_row) -> bool: # catalog_row : type
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
        
        # Generate a unique file ID
        file_id = self._generate_file_id(catalog_row)
        output_path = os.path.join(self.fits_dir, path)
        
        # Add file to database if not already tracked
        self.db_manager.add_file(
            file_id=file_id,
            sector=self.sector,
            pipeline=self.pipeline,
            file_path=output_path
        )

        try:
            subprocess.run(cmd, shell=True, check=True, text=True, capture_output=True)
            
            # Update database with success status and file info
            if os.path.exists(output_path):
                file_size = os.path.getsize(output_path)
                checksum = self._calculate_checksum(output_path)
                self.db_manager.update_file_info(output_path, file_size, checksum)
                self.db_manager.update_status(file_id, 'success')
            else:
                self.db_manager.update_status(file_id, 'failed', 'File not created')
                return False
                
            return True
        
        except subprocess.CalledProcessError as e:
            error_msg = f"Download failed: {e.stderr}"
            self.db_manager.update_status(file_id, 'failed', error_msg)
            logger.error(f"Error downloading {url}: {error_msg}")
            return False
    
    def _generate_file_id(self, catalog_row: Row) -> str:
        """Generate a unique ID for a file based on catalog row data"""
        # Use the primary identifier from the catalog row
        # This will be different for each pipeline (TIC_ID or GAIADR3_ID)
        primary_id = str(catalog_row[self.catalog_column_names[0]])
        return f"{self.pipeline}_{primary_id}_{self.sector_str}"
    
    def _calculate_checksum(self, file_path: str) -> str:
        """Calculate MD5 checksum for a file"""
        if not os.path.exists(file_path):
            return None
            
        md5_hash = hashlib.md5()
        with open(file_path, "rb") as f:
            # Read in chunks to handle large files
            for chunk in iter(lambda: f.read(4096), b""):
                md5_hash.update(chunk)
        return md5_hash.hexdigest()

    @abstractmethod
    def processing_fn(
            self,
            del_fits: bool = True
    ):
        pass

    def save_in_standard_format(self, args: tuple[Table, str], del_fits: bool = True, min_samples: int = None) -> bool:
        '''
        Save the standardised batch of light curves dict in a hdf5 file 

        Parameters
        ----------
        args: tuple, tuple of arguments: (subcatalog, output_filename)
        del_fits: bool, whether to delete the fits files after processing
        min_samples: int, optional, minimum number of samples required for a light curve to be included (default: None, which includes all)

        Returns
        -------
        success: bool, True if the file was saved successfully, False otherwise
        '''

        subcatalog, output_filename = args
        
        if not os.path.exists(os.path.dirname(output_filename)):
            os.makedirs(os.path.dirname(output_filename))

        results = [] 
        skipped_count = 0
        total_skipped_samples = 0
        
        for row in tqdm(subcatalog):
            result = self.processing_fn(row, del_fits=del_fits)
            if result is not None:  # Usually for files not found.
                # Filter by minimum number of samples only if min_samples is specified
                if min_samples is not None:
                    time_data = result.get('time', [])
                    # Filter out NaN values when counting samples
                    valid_samples = np.sum(~np.isnan(time_data))
                    
                    if valid_samples < min_samples:
                        # Skip this light curve due to insufficient samples
                        total_skipped_samples += valid_samples
                        skipped_count += 1
                        continue
                
                # Add sector number to each light curve
                result['sector'] = self.sector
                
                # Add all catalog features to the result
                for col in row.colnames:
                    # Skip columns that are already in the result or that we don't want to duplicate
                    if col not in result and col not in ['time', 'flux', 'flux_err', 'quality']:
                        # Convert to appropriate type for storage
                        if isinstance(row[col], (int, float, str, bool)):
                            result[col] = row[col]
                
                results.append(result)

        if not results:
            if min_samples is not None:
                logger.warning(f"No valid light curves found for output file {output_filename}. Skipped {skipped_count} light curves due to sample count.")
            else:
                logger.warning(f"No valid light curves found for output file {output_filename}.")
            return 0

        if min_samples is not None and skipped_count > 0:
            avg_skipped = total_skipped_samples / skipped_count if skipped_count > 0 else 0
            logger.info(f"Kept {len(results)} light curves, skipped {skipped_count} due to insufficient samples. Skipped curves had {avg_skipped:.1f} samples on average.")
        else:
            logger.info(f"Processed {len(results)} light curves with no filtering.")
        
        # Create a list of all keys across all results
        all_keys = set()
        for result in results:
            all_keys.update(result.keys())
        
        # Ensure all results have all keys
        for result in results:
            for key in all_keys:
                if key not in result:
                    if any(isinstance(r.get(key, None), np.ndarray) for r in results if key in r):
                        # If this is an array field in other results, create an empty array
                        result[key] = np.array([])
                    else:
                        # Otherwise set to None
                        result[key] = None
        
        # Find the maximum length of any array in the results
        max_length = 0
        for result in results:
            for key, value in result.items():
                if isinstance(value, np.ndarray):
                    max_length = max(max_length, len(value))
        
        # Pad all arrays to the same length
        for result in results:
            for key, value in result.items():
                if isinstance(value, np.ndarray) and len(value) < max_length:
                    result[key] = np.pad(value, (0, max_length - len(value)), mode='constant', constant_values=np.nan)
        
        # Create the table with all fields
        lightcurves = Table({k: [d.get(k) for d in results] for k in all_keys})
        lightcurves.convert_unicode_to_bytestring()

        with h5py.File(output_filename, 'w') as hdf5_file:
            # Add sector as a file attribute as well
            hdf5_file.attrs['sector'] = self.sector
            # Add min_samples as an attribute for reference (or None if not filtering)
            if min_samples is not None:
                hdf5_file.attrs['min_samples'] = min_samples
            
            # Add pipeline information
            hdf5_file.attrs['pipeline'] = self.pipeline
            
            # Store all columns
            for key in lightcurves.colnames:
                try:
                    hdf5_file.create_dataset(key, data=lightcurves[key])
                except Exception as e:
                    logger.warning(f"Could not save column {key}: {str(e)}")
        
        logger.info(f"Saved {len(results)} light curves to {output_filename} with {len(all_keys)} features")
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
    
    async def download_fits_file(self, session, url, output_path, catalog_row, max_retries=3, base_delay=2):
        """Asynchronously download a FITS file and track in database"""
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Generate file ID and add to database
        file_id = self._generate_file_id(catalog_row)
        self.db_manager.add_file(
            file_id=file_id,
            sector=self.sector,
            pipeline=self.pipeline,
            file_path=output_path
        )

        for attempt in range(max_retries):
            try:
                async with session.get(url) as response:
                    if response.status == 200:
                        with open(output_path, 'wb') as f:
                            async for chunk in response.content.iter_chunked(_CHUNK_SIZE):  # 8KB chunks
                                f.write(chunk)
                        
                        # Update database with success
                        file_size = os.path.getsize(output_path)
                        checksum = self._calculate_checksum(output_path)
                        self.db_manager.update_file_info(output_path, file_size, checksum)
                        self.db_manager.update_status(file_id, 'success')
                        return output_path
                    elif response.status == 429:
                        delay = base_delay * 2**attempt # exponential backoff for finding required delay
                        logger.warning(f"Rate limited. Waiting {delay} seconds...")
                        await asyncio.sleep(delay)
                        continue
                    else:
                        error_msg = f"Failed to download: Status {response.status}"
                        logger.error(f"{error_msg} for {url}")
                        if attempt < max_retries - 1:
                            delay = base_delay * (2 ** attempt)
                            logger.info(f"Retrying in {delay} seconds... (Attempt {attempt + 1}/{max_retries})")
                            await asyncio.sleep(delay)
                            continue
                        else:
                            self.db_manager.update_status(file_id, 'failed', error_msg)

            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                error_msg = f"Error: {str(e)}"
                if attempt < max_retries - 1:
                    delay = base_delay * (2 ** attempt)
                    logger.warning(f"{error_msg}. Retrying in {delay} seconds... (Attempt {attempt + 1}/{max_retries})")
                    await asyncio.sleep(delay)
                else:
                    logger.error(f"Failed after {max_retries} attempts: {error_msg}")
                    self.db_manager.update_status(file_id, 'failed', error_msg)
                    return False
        
        # If we get here, all attempts failed
        self.db_manager.update_status(file_id, 'failed', "Max retries exceeded")
        return False
    
    async def download_batch(self, catalog_batch: Table) -> list[bool]:
        if not os.path.exists(self.fits_dir):
            os.makedirs(self.fits_dir, exist_ok=True)

        urls_and_paths = [self.fits_url(row) for row in catalog_batch]
        urls, paths = zip(*urls_and_paths)

        async with aiohttp.ClientSession() as session:
            tasks = []
            for i, url in enumerate(urls):
                output_path = os.path.join(self.fits_dir, paths[i])
                task = asyncio.create_task(
                    self.download_fits_file(
                        session, 
                        url, 
                        output_path,
                        catalog_batch[i]
                    )
                )
                tasks.append(task)
            
            completed_files = await asyncio.gather(*tasks)
            success_count = sum(1 for f in completed_files if f)
            logger.info(f"Downloaded {success_count} of {len(completed_files)} files successfully")
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
                
        success_count = sum(1 for r in results if r)
        logger.info(f"Downloaded {success_count} of {len(results)} files successfully")
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
        catalog.rename_column('RA', 'ra')
        catalog.rename_column('DEC', 'dec')

        if save_catalog:
            self.catalog_fp = os.path.join(self.data_path, f'{self.sector_str}_catalog{"_tiny" if tiny else ""}.hdf5')
            catalog.write(self.catalog_fp , format='hdf5', overwrite=True, path=self.catalog_fp )
            print(f"Saved catalog to {self.catalog_fp }")
        return catalog
        
    def convert_fits_to_standard_format(self, catalog: Table, min_samples: int = None) -> list[bool]:
        '''
        Convert the fits light curves to the standard format and save them in a hdf5 file

        Parameters
        ----------
        catalog: astropy.Table, sector catalog
        min_samples: int, optional, minimum number of samples required for a light curve to be included

        Returns
        -------
        results: list, list of booleans indicating the success of the conversion for each light curve
        '''

        # Group the catalog by healpix - this returns only a grouped table
        grouped_catalog = catalog.group_by(['healpix'])
        
        # Log the grouped catalog for debugging
        logger.debug(f"Grouped catalog has {len(grouped_catalog.groups)} groups")

        map_args = []
        for group in grouped_catalog.groups: 
            group_filename = os.path.join(self.hdf5_output_dir, '{}/healpix={}/001-of-001.hdf5'.format(self.pipeline, group['healpix'][0]))
            map_args.append((group, group_filename))

        with Pool(self.n_processes) as pool:
            # Use imap with a wrapper for save_in_standard_format instead of starmap with lambda
            results = list(tqdm(pool.imap(
                self._save_in_standard_format_wrapper, 
                [(args, min_samples) for args in map_args]), 
                total=len(map_args)))

        if sum(results) != len(map_args):
            logger.warning("There was an error in the parallel processing of the fits files to standard format, some files may not have been processed correctly")
        return results

    def _save_in_standard_format_wrapper(self, args_with_min_samples):
        """Wrapper function for save_in_standard_format to use with pool.imap"""
        args, min_samples = args_with_min_samples
        return self.save_in_standard_format(args, min_samples=min_samples)

    def batcher(self, seq: list, batch_size: int) -> list[list]:
        return (seq[pos:pos + batch_size] for pos in range(0, len(seq), batch_size))

    def batched_download(self, catalog: Table, tiny: bool) -> list[list[bool]]:
        if tiny:
            results = self.download_sector_catalog_lightcurves(catalog=catalog[:_TINY_SIZE])
            file_count = len(catalog[:_TINY_SIZE])
        else:
            catalog_len = len(catalog)
            results = []
            for batch in tqdm(self.batcher(catalog, _BATCH_SIZE), total = catalog_len // _BATCH_SIZE):
                try:
                    batch_results = self.download_sector_catalog_lightcurves(batch)
                    results.append(batch_results)
                except Exception as e:
                    error_msg = f"Error downloading batch: {str(e)}"
                    logger.error(error_msg)
                    print(f"{error_msg}. Waiting {PAUSE_TIME} seconds before retrying...")
                    time.sleep(PAUSE_TIME)
                    # Try again
                    batch_results = self.download_sector_catalog_lightcurves(batch)
                    results.append(batch_results)

        return results
        
    def download_sector(
            self,
            tiny: bool = True, 
            show_progress: bool = False,
            save_catalog: bool = False,
            clean_up: bool = True,
            resume_failed: bool = True,
            skip_completed: bool = True,
            min_samples: int = None,  # Default to None (no filtering)
            use_mast_direct: bool = True  # New parameter to control download method
    ) -> bool:
        '''
        Download the sector data from the MAST site and save it in the standard format 

        Parameters
        ----------
        tiny: bool, if True, only use a small sample of 100 objects for testing
        show_progress: bool, if True, show the progress of the download
        save_catalog: bool, if True, save the catalog to disk
        clean_up: bool, if True, clean up temporary files after processing
        resume_failed: bool, if True, attempt to resume any failed downloads
        skip_completed: bool, if True, skip sectors that have been previously completed
        min_samples: int, optional, minimum number of samples required (default: None, which includes all light curves)
        use_mast_direct: bool, if True, use direct MAST download instead of curl commands

        Returns
        -------
        success: bool, True if the download was successful, False otherwise
        '''
        
        if min_samples is not None:
            logger.info(f"Starting download for sector {self.sector} with filtering: min_samples={min_samples}")
        else:
            logger.info(f"Starting download for sector {self.sector} with no sample filtering")
        
        # Check if this sector is already completed
        if skip_completed and self.db_manager.is_sector_complete(self.sector, self.pipeline):
            logger.info(f"Sector {self.sector} for pipeline {self.pipeline} already completed. Skipping.")
            return True
        
        # Check if we should resume failed downloads first
        if resume_failed:
            logger.info("Checking for failed downloads to resume")
            self.resume_failed_downloads()

        # Use direct MAST download if requested
        if use_mast_direct:
            
            success = self.download_from_mast(tiny=tiny)
            
            # Mark sector as complete if successful
            # if success:
            try:
                # Get download statistics
                stats = self.get_download_stats()
                logger.info(f"Final download statistics: {stats}")
                
                # Mark sector as complete
                file_count = stats.get('success', 0) + stats.get('failed', 0)
                success_count = stats.get('success', 0)
                self.db_manager.mark_sector_complete(
                    self.sector, 
                    self.pipeline, 
                    file_count, 
                    success_count
                )
                logger.info(f"Marked sector {self.sector} as complete for pipeline {self.pipeline}")
                
                # Clean-up of fits files
                if clean_up:
                    self.clean_up()
                    
                return True
            except Exception as e:
                logger.warning(f"Direct MAST download failed, falling back to traditional method: {e}")
        
        # If direct download was not requested or failed, use the traditional method
        logger.info("Using traditional download method with shell scripts")
        
        # Download the sh file from the site
        self.download_sh_script(show_progress) 

        # Download the target list csv file from the site
        self.download_target_csv_file(show_progress)

        # Create the sector catalog
        catalog = self.create_sector_catalog(save_catalog=save_catalog, tiny=tiny)
        
        # Download the fits light curves using the sector catalog
        if tiny:
            results = self.batched_download(catalog[:_TINY_SIZE], tiny)
            file_count = len(catalog[:_TINY_SIZE])
        else:
            results = self.batched_download(catalog, tiny)
            file_count = len(catalog)
        
        # Count successful downloads
        success_count = sum(1 for batch in results for r in batch if r)
        
        # Process fits to standard format
        if tiny:
            self.convert_fits_to_standard_format(catalog[:_TINY_SIZE], min_samples=min_samples)
        else:
            self.convert_fits_to_standard_format(catalog, min_samples=min_samples)

        # Print download statistics
        stats = self.get_download_stats()
        logger.info(f"Final download statistics: {stats}")

        # Mark sector as complete
        self.db_manager.mark_sector_complete(
            self.sector, 
            self.pipeline, 
            file_count, 
            success_count
        )
        logger.info(f"Marked sector {self.sector} as complete for pipeline {self.pipeline}")

        # Clean-up of fits, .sh and .csv files
        if clean_up:
            self.clean_up()

        return True
    
    def clean_up(self) -> None:
        '''
        Clean-up for the fits, .sh and .csv files to free up disk space after the parent sample has been built.
        '''
        # Only remove files that exist
        for fp_attr in ['sh_fp', 'csv_fp', 'catalog_fp']:
            if hasattr(self, fp_attr) and getattr(self, fp_attr) is not None:
                fp = getattr(self, fp_attr)
                if os.path.exists(fp):
                    try:
                        os.remove(fp)
                        logger.debug(f"Removed file: {fp}")
                    except Exception as e:
                        logger.warning(f"Failed to remove file {fp}: {e}")

        # Clean up fits directory if it exists
        if hasattr(self, 'fits_dir') and self.fits_dir is not None and os.path.exists(self.fits_dir):
            logger.info(f"Cleaning up fits directory: {self.fits_dir}")  
            try:
                shutil.rmtree(self.fits_dir, ignore_errors=True)
            except Exception as e:
                logger.warning(f"Failed to remove fits directory {self.fits_dir}: {e}")

        return

    def resume_failed_downloads(self):
        """Resume downloading files that previously failed"""
        failed_downloads = self.db_manager.get_failed_downloads()
        
        if failed_downloads.empty:
            logger.info("No failed downloads to resume")
            return []
        
        logger.info(f"Resuming {len(failed_downloads)} failed downloads")
        
        # Create a catalog-like structure from the failed downloads
        # This is pipeline-specific, so we'll need to implement it in each subclass
        catalog = self._create_catalog_from_failed(failed_downloads)
        
        if catalog is None or len(catalog) == 0:
            logger.warning("Could not create catalog from failed downloads")
            return []
        
        # Download the files
        return self.download_sector_catalog_lightcurves(catalog)
    
    @abstractmethod
    def _create_catalog_from_failed(self, failed_downloads):
        """Create a catalog from failed downloads for resuming"""
        pass
    
    def get_download_stats(self):
        """Get statistics about downloads"""
        stats = self.db_manager.get_download_stats()
        logger.info(f"Download statistics: {stats}")
        return stats

    def download_from_mast(self, tiny=False):
        """
        Download light curves directly from MAST using astroquery instead of curl commands
        """
        logger.info(f"Using direct MAST download for {self.pipeline} sector {self.sector}")
        
        try:
            # Different query parameters based on pipeline
            if self.pipeline == 'SPOC':
                obs_table = Observations.query_criteria(
                    provenance_name='TESS-SPOC',
                    sequence_number=self.sector,
                    dataproduct_type='timeseries',
                    dataRights='PUBLIC'
                )
            elif self.pipeline == 'QLP':
                obs_table = Observations.query_criteria(
                    project='HLSP',
                    provenance_name='QLP',
                    sequence_number=self.sector,
                    dataproduct_type='timeseries',
                    dataRights='PUBLIC'
                )
            elif self.pipeline == 'TGLC':
                obs_table = Observations.query_criteria(
                    project='HLSP',
                    hlsp_project='TGLC',
                    sequence_number=self.sector,
                    dataRights='PUBLIC'
                )
            else:
                raise ValueError(f"Unsupported pipeline: {self.pipeline}")
            
            logger.info(f"Found {len(obs_table)} observations for sector {self.sector}")
            
            # For tiny mode, we need to be smarter about sampling
            if tiny:
                # Query a larger sample first to ensure we get enough targets with light curves
                sample_size = min(1000, len(obs_table))
                logger.info(f"Using a sample of {sample_size} targets to find {_TINY_SIZE} light curves")
                obs_table = obs_table[:sample_size]
            
            # Retrieve product list in batches to avoid timeouts
            batch_size = 4096
            table_len = len(obs_table)
            logger.info(f'Retrieving data product download links for {table_len} targets in batches of {batch_size}')
            
            product_list = []
            for batch in tqdm(self.batcher(obs_table, batch_size), total=(table_len // batch_size) + 1):
                try:
                    batch_products = Observations.get_product_list(batch)
                    if len(batch_products) > 0:
                        product_list.append(batch_products)
                except Exception as e:
                    logger.warning(f'Error retrieving product list: {str(e)}. Waiting {PAUSE_TIME} seconds before retrying...')
                    time.sleep(PAUSE_TIME)
                    try:
                        batch_products = Observations.get_product_list(batch)
                        if len(batch_products) > 0:
                            product_list.append(batch_products)
                    except Exception as retry_e:
                        logger.error(f'Retry failed: {str(retry_e)}')
            
            if not product_list:
                logger.warning("No products found for download")
                return False
            
            try:
                products = vstack(product_list)
                logger.info(f"Retrieved {len(products)} products before filtering")
            except Exception as e:
                logger.error(f"Error stacking product lists: {str(e)}")
                return False
            
            # Filter products based on pipeline - use more flexible filtering
            try:
                if self.pipeline == 'SPOC':
                    # Log the unique values to help debug
                    unique_groups = np.unique(products['productSubGroupDescription'].astype(str))
                    logger.info(f"Available product groups: {unique_groups}")
                    
                    # Try a more flexible filter for SPOC
                    mask = np.zeros(len(products), dtype=bool)
                    
                    # Check for light curve indicators in product description
                    for term in ['LC', 'LIGHTCURVE', 'LIGHT_CURVE']:
                        mask |= np.char.find(products['productSubGroupDescription'].astype(str), term) >= 0
                    
                    # Also check filenames for light curves
                    filename_mask = np.char.find(products['productFilename'].astype(str), 'lc.fits') >= 0
                    mask |= filename_mask
                    
                    products = products[mask]
                elif self.pipeline == 'QLP':
                    products = products[np.char.find(products['productFilename'].astype(str), 'llc.fits') >= 0]
                elif self.pipeline == 'TGLC':
                    products = products[np.char.find(products['productFilename'].astype(str), 'llc.fits') >= 0]
            except Exception as e:
                logger.error(f"Error filtering products: {str(e)}")
                return False
            
            logger.info(f"Found {len(products)} products after filtering")
            
            # For tiny mode, limit to _TINY_SIZE products
            if tiny and len(products) > _TINY_SIZE:
                logger.info(f"Limiting to {_TINY_SIZE} products for tiny mode")
                products = products[:_TINY_SIZE]
            
            if len(products) == 0:
                logger.warning("No products found after filtering")
                return False
            
            # Create output directory
            os.makedirs(self.fits_dir, exist_ok=True)
            
            # Download products
            logger.info(f"Downloading {len(products)} light curves to {self.fits_dir}")
            try:
                manifest = Observations.download_products(
                    products,
                    download_dir=self.fits_dir,
                    flat=True
                )
            except Exception as e:
                logger.error(f"Error downloading products: {str(e)}")
                return False
            
            # Check if manifest is None (no products downloaded)
            if manifest is None:
                logger.warning("No products were downloaded")
                return False
            
            # Process downloaded files
            success_count = 0
            manifest = manifest.to_pandas()
            for _, row in manifest.iterrows():
                if row['Status'] == 'COMPLETE':
                    try:
                        # Generate a file ID for database tracking
                        try:
                            if self.pipeline == 'SPOC':
                                # Extract TIC ID from filename
                                filename = os.path.basename(row['Local Path'])
                                if '_' in filename and '-' in filename:
                                    target_id = filename.split('_')[4].split('-')[0]
                                elif '-' in filename:
                                    target_id = filename.split('-')[1].split('_')[0]
                                else:
                                    logger.warning(f"Could not parse filename format: {filename}")
                                    continue
                                file_id = f"SPOC_{target_id}_{self.sector_str}"
                            elif self.pipeline == 'QLP':
                                # Extract TIC ID from filename
                                filename = os.path.basename(row['Local Path'])
                                target_id = filename.split('-')[1].split('_')[0]
                                file_id = f"QLP_{target_id}_{self.sector_str}"
                            elif self.pipeline == 'TGLC':
                                # Extract GAIA ID from filename
                                filename = os.path.basename(row['Local Path'])
                                target_id = filename.split('-')[1]
                                file_id = f"{self.pipeline}_{target_id}_{self.sector_str}"
                        except Exception as e:
                            logger.warning(f"Error extracting target ID from filename {row['Local Path']}: {str(e)}")
                            continue
                        
                        # Add to database
                        self.db_manager.add_file(
                            file_id=file_id,
                            sector=self.sector,
                            pipeline=self.pipeline,
                            file_path=row['Local Path']
                        )
                        
                        # Update status
                        file_size = os.path.getsize(row['Local Path'])
                        checksum = self._calculate_checksum(row['Local Path'])
                        self.db_manager.update_file_info(row['Local Path'], file_size, checksum)
                        self.db_manager.update_status(file_id, 'success')
                        
                        success_count += 1
                    except Exception as e:
                        logger.error(f"Error processing downloaded file {row['Local Path']}: {str(e)}")
                        continue
            
            logger.info(f"Successfully downloaded {success_count} of {len(products)} files")
            
            if success_count == 0:
                logger.warning("No files were successfully downloaded")
                return False
            
            # Create a catalog from the downloaded files for further processing
            try:
                # Create detailed debug information
                logger.debug("Creating catalog from downloaded files")
                catalog = Table()
                
                # Initialize ID lists
                tic_ids = []
                gaia_ids = []

                
                # Extract IDs from filenames with detailed logging
                if isinstance(manifest, Table):
                    manifest = manifest.to_pandas()
                for _, row in manifest.iterrows():
                    if row['Status'] == 'COMPLETE':
                        filename = os.path.basename(row['Local Path'])
                        logger.debug(f"Processing filename: {filename}")
                        
                        try:
                            if self.pipeline == 'SPOC':
                                if '_' in filename and '-' in filename:
                                    tic_id = int(filename.split('_')[4].split('-')[0])
                                    logger.debug(f"Extracted TIC ID: {tic_id} from format: _X-")
                                elif '-' in filename:
                                    tic_id = int(filename.split('-')[1].split('_')[0])
                                    logger.debug(f"Extracted TIC ID: {tic_id} from format: -X_")
                                else:
                                    logger.warning(f"Unrecognized filename format: {filename}")
                                    continue
                                tic_ids.append(tic_id)
                            elif self.pipeline == 'QLP':
                                tic_id = int(filename.split('-')[1].split('_')[0])
                                logger.debug(f"Extracted TIC ID: {tic_id}")
                                tic_ids.append(tic_id)
                            elif self.pipeline == 'TGLC':
                                gaia_id = int(filename.split('-')[1])
                                logger.debug(f"Extracted GAIA ID: {gaia_id}")
                                gaia_ids.append(gaia_id)
                        except (IndexError, ValueError) as e:
                            logger.warning(f"Could not extract ID from filename: {filename}. Error: {str(e)}")
                            continue
                
                # Add IDs to catalog
                if self.pipeline in ['SPOC', 'QLP'] and tic_ids:
                    logger.debug(f"Adding {len(tic_ids)} TIC IDs to catalog")
                    catalog['TIC_ID'] = tic_ids
                elif self.pipeline == 'TGLC' and gaia_ids:
                    logger.debug(f"Adding {len(gaia_ids)} GAIA IDs to catalog")
                    catalog['GAIADR3_ID'] = gaia_ids
                
                # Add RA/DEC from the observation table
                if len(catalog) > 0:
                    logger.debug(f"Adding RA/DEC for {len(catalog)} targets")
                    # Match targets to observation table
                    ra_list = []
                    dec_list = []
                    
                    for i, row in enumerate(catalog):
                        target_id = None
                        if self.pipeline == 'SPOC' or self.pipeline == 'QLP':
                            if 'TIC_ID' in row.colnames:
                                target_id = str(row['TIC_ID'])
                                logger.debug(f"Looking up TIC ID: {target_id}")
                        elif self.pipeline == 'TGLC':
                            if 'GAIADR3_ID' in row.colnames:
                                target_id = str(row['GAIADR3_ID'])
                                logger.debug(f"Looking up GAIA ID: {target_id}")
                        
                        if target_id is not None:
                            # Find matches in observation table
                            matches = obs_table[np.char.find(obs_table['target_name'].astype(str), target_id) >= 0]
                            
                            if len(matches) > 0:
                                ra_list.append(float(matches[0]['s_ra']))
                                dec_list.append(float(matches[0]['s_dec']))
                                logger.debug(f"Found match for {target_id}: RA={matches[0]['s_ra']}, DEC={matches[0]['s_dec']}")
                            else:
                                # If no match, use a default value
                                ra_list.append(0.0)
                                dec_list.append(0.0)
                                logger.debug(f"No match found for {target_id}, using default RA/DEC")
                        else:
                            # If no target ID, use a default value
                            ra_list.append(0.0)
                            dec_list.append(0.0)
                            logger.debug(f"No target ID for row {i}, using default RA/DEC")
                    
                    catalog['ra'] = ra_list
                    catalog['dec'] = dec_list
                    
                    # Add healpix
                    logger.debug("Calculating healpix values")
                    catalog['healpix'] = hp.ang2pix(_healpix_nside, catalog['ra'], catalog['dec'], lonlat=True, nest=True)
                    
                    # Save catalog for later use
                    self.catalog_fp = os.path.join(self.data_path, f'{self.sector_str}_catalog{"_tiny" if tiny else ""}.hdf5')
                    logger.debug(f"Saving catalog to {self.catalog_fp}")
                    catalog.write(self.catalog_fp, format='hdf5', overwrite=True, path='catalog')
                    logger.info(f"Saved catalog with {len(catalog)} entries to {self.catalog_fp}")
                    
                    # Process the downloaded files
                    logger.info("Converting fits files to standard format")
                    
                    # Create a modified version of convert_fits_to_standard_format that doesn't rely on fp1, fp2, etc.
                    self.convert_direct_download_to_standard_format(catalog)
                    return True
                else:
                    logger.error("Could not create catalog from downloaded files - catalog is empty")
                    return False
                
            except Exception as e:
                import traceback
                logger.error(f"Error creating catalog from downloaded files: {str(e)}")
                logger.error(f"Traceback: {traceback.format_exc()}")
                return False
            
        except Exception as e:
            import traceback
            logger.error(f"Error in direct MAST download: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            return False

    def convert_direct_download_to_standard_format(self, catalog: Table, min_samples: int = None) -> list[bool]:
        '''
        Convert the fits light curves from direct download to the standard format and save them in a hdf5 file
        This version doesn't rely on fp1, fp2, etc. fields in the catalog
        
        Parameters
        ----------
        catalog: astropy.Table, sector catalog
        min_samples: int, optional, minimum number of samples required for a light curve to be included
        
        Returns
        -------
        results: list, list of booleans indicating the success of the conversion for each light curve
        '''
        
        # Group the catalog by healpix - this returns only a grouped table
        grouped_catalog = catalog.group_by(['healpix'])
        
        # Log the grouped catalog for debugging
        logger.debug(f"Grouped catalog has {len(grouped_catalog.groups)} groups")
        
        map_args = []
        for group in grouped_catalog.groups: 
            group_filename = os.path.join(self.hdf5_output_dir, '{}/healpix={}/001-of-001.hdf5'.format(self.pipeline, group['healpix'][0]))
            map_args.append((group, group_filename))
        
        with Pool(self.n_processes) as pool:
            # Use imap with a wrapper for direct_save_in_standard_format
            results = list(tqdm(pool.imap(
                self._direct_save_in_standard_format_wrapper, 
                [(args, min_samples) for args in map_args]), 
                total=len(map_args)))
        
        if sum(results) != len(map_args):
            logger.warning("There was an error in the parallel processing of the fits files to standard format, some files may not have been processed correctly")
        return results

    def _direct_save_in_standard_format_wrapper(self, args_with_min_samples):
        """Wrapper function for direct_save_in_standard_format to use with pool.imap"""
        args, min_samples = args_with_min_samples
        return self.direct_save_in_standard_format(args, min_samples=min_samples)

    def direct_save_in_standard_format(self, args: tuple[Table, str], del_fits: bool = True, min_samples: int = None) -> bool:
        '''
        Save the standardised batch of light curves dict in a hdf5 file for direct downloads
        This version doesn't rely on fp1, fp2, etc. fields in the catalog
        
        Parameters
        ----------
        args: tuple, tuple of arguments: (subcatalog, output_filename)
        del_fits: bool, whether to delete the fits files after processing
        min_samples: int, optional, minimum number of samples required for a light curve to be included
        
        Returns
        -------
        success: bool, True if the file was saved successfully, False otherwise
        '''
        
        subcatalog, output_filename = args
        
        if not os.path.exists(os.path.dirname(output_filename)):
            os.makedirs(os.path.dirname(output_filename))
        
        results = [] 
        skipped_count = 0
        total_skipped_samples = 0
        
        for row in tqdm(subcatalog):
            # For direct downloads, we need to find the file path differently
            # Instead of using fits_url which requires fp1, fp2, etc.
            result = self.direct_processing_fn(row, del_fits=del_fits)
            
            if result is not None:  # Usually for files not found.
                # Filter by minimum number of samples only if min_samples is specified
                if min_samples is not None:
                    time_data = result.get('time', [])
                    # Filter out NaN values when counting samples
                    valid_samples = np.sum(~np.isnan(time_data))
                    
                    if valid_samples < min_samples:
                        # Skip this light curve due to insufficient samples
                        total_skipped_samples += valid_samples
                        skipped_count += 1
                        continue
                
                # Add sector number to each light curve
                result['sector'] = self.sector
                
                # Add all catalog features to the result
                for col in row.colnames:
                    # Skip columns that are already in the result or that we don't want to duplicate
                    if col not in result and col not in ['time', 'flux', 'flux_err', 'quality']:
                        # Convert to appropriate type for storage
                        if isinstance(row[col], (int, float, str, bool)):
                            result[col] = row[col]
                
                results.append(result)
        
        if not results:
            if min_samples is not None:
                logger.warning(f"No valid light curves found for output file {output_filename}. Skipped {skipped_count} light curves due to sample count.")
            else:
                logger.warning(f"No valid light curves found for output file {output_filename}.")
            return 0
        
        if min_samples is not None and skipped_count > 0:
            avg_skipped = total_skipped_samples / skipped_count if skipped_count > 0 else 0
            logger.info(f"Kept {len(results)} light curves, skipped {skipped_count} due to insufficient samples. Skipped curves had {avg_skipped:.1f} samples on average.")
        else:
            logger.info(f"Processed {len(results)} light curves with no filtering.")
        
        # Create a list of all keys across all results
        all_keys = set()
        for result in results:
            all_keys.update(result.keys())
        
        # Ensure all results have all keys
        for result in results:
            for key in all_keys:
                if key not in result:
                    if any(isinstance(r.get(key, None), np.ndarray) for r in results if key in r):
                        # If this is an array field in other results, create an empty array
                        result[key] = np.array([])
                    else:
                        # Otherwise set to None
                        result[key] = None
        
        # Find the maximum length of any array in the results
        max_length = 0
        for result in results:
            for key, value in result.items():
                if isinstance(value, np.ndarray):
                    max_length = max(max_length, len(value))
        
        # Pad all arrays to the same length
        for result in results:
            for key, value in result.items():
                if isinstance(value, np.ndarray) and len(value) < max_length:
                    result[key] = np.pad(value, (0, max_length - len(value)), mode='constant', constant_values=np.nan)
        
        # Create the table with all fields
        lightcurves = Table({k: [d.get(k) for d in results] for k in all_keys})
        lightcurves.convert_unicode_to_bytestring()
        
        with h5py.File(output_filename, 'w') as hdf5_file:
            # Add sector as a file attribute as well
            hdf5_file.attrs['sector'] = self.sector
            # Add min_samples as an attribute for reference (or None if not filtering)
            if min_samples is not None:
                hdf5_file.attrs['min_samples'] = min_samples
            
            # Add pipeline information
            hdf5_file.attrs['pipeline'] = self.pipeline
            
            # Store all columns
            for key in lightcurves.colnames:
                try:
                    hdf5_file.create_dataset(key, data=lightcurves[key])
                except Exception as e:
                    logger.warning(f"Could not save column {key}: {str(e)}")
        
        logger.info(f"Saved {len(results)} light curves to {output_filename} with {len(all_keys)} features")
        return 1


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
                'ra': ra: float,
                'dec': dec # More columns maybe required...
            }
        '''

        fits_fp = os.path.join(self.fits_dir, self.fits_url(catalog_row)[1])
        try:
            with fits.open(fits_fp, mode='readonly', memmap=True) as hdu:
                entry = {
                    'object_id': catalog_row['TIC_ID'],
                    'time': hdu['LIGHTCURVE'].data['TIME'],  # Fixed: use TIME instead of SAP_FLUX
                    'flux': hdu['LIGHTCURVE'].data['SAP_FLUX'], #Note: PDCSAP_FLUX is processed.
                    'flux_err':  hdu['LIGHTCURVE'].data['SAP_FLUX_ERR'],
                    'quality': np.asarray(hdu['LIGHTCURVE'].data['QUALITY'], dtype='int32'),
                    'ra': hdu[1].header['ra_obj'],
                    'dec': hdu[1].header['dec_obj']
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

    def _create_catalog_from_failed(self, failed_downloads):
        """Create a catalog from failed downloads for SPOC"""
        if failed_downloads.empty:
            return None
            
        rows = []
        if isinstance(failed_downloads, Table):
            failed_downloads = failed_downloads.to_pandas()
        for _, row in failed_downloads.iterrows():
            # Extract TIC_ID from file_id
            parts = row['file_id'].split('_')
            if len(parts) >= 2:
                tic_id = int(parts[1])
                
                # Extract fp values from file_path
                path_parts = row['file_path'].split('/')
                if len(path_parts) >= 5:  # Ensure path has enough parts
                    fp_values = path_parts[-5:-1]  # Extract fp1, fp2, fp3, fp4
                    if len(fp_values) == 4:
                        rows.append([tic_id] + [int(fp) for fp in fp_values])
        
        if not rows:
            return None
            
        return Table(rows=rows, names=self.catalog_column_names)

    # Add to SPOC_Downloader class
    def direct_processing_fn(
            self,
            catalog_row : Row,
            del_fits : bool = True
    ) -> dict:
        ''' 
        Process a single light curve file for direct downloads
        
        Parameters
        ----------
        row: astropy Row, a single row from the sector catalog
        
        Returns
        ------- 
        lightcurve: dict, light curve in the standard format
        '''
        
        # For direct downloads, we need to find the file by searching the fits_dir
        # for files matching the TIC_ID
        tic_id = catalog_row['TIC_ID']
        
        # Search for the file in the fits_dir
        fits_files = []
        for root, _, files in os.walk(self.fits_dir):
            for file in files:
                if file.endswith('.fits') and str(tic_id) in file:
                    fits_files.append(os.path.join(root, file))
        
        if not fits_files:
            logger.warning(f"No fits file found for TIC_ID {tic_id}")
            return None
        
        # Use the first matching file
        fits_fp = fits_files[0]
        
        try:
            with fits.open(fits_fp, mode='readonly', memmap=True) as hdu:
                entry = {
                    'object_id': tic_id,
                    'time': hdu['LIGHTCURVE'].data['TIME'],  # Fixed: use TIME instead of SAP_FLUX
                    'flux': hdu['LIGHTCURVE'].data['SAP_FLUX'],
                    'flux_err':  hdu['LIGHTCURVE'].data['SAP_FLUX_ERR'],
                    'quality': np.asarray(hdu['LIGHTCURVE'].data['QUALITY'], dtype='int32'),
                    'ra': hdu[1].header['ra_obj'],
                    'dec': hdu[1].header['dec_obj']
                }
                if del_fits:
                    os.remove(fits_fp)
                    try:
                        os.rmdir(os.path.dirname(fits_fp))
                    except:
                        pass
                return entry

        except FileNotFoundError:
            logger.warning(f"File not found: {fits_fp}")
            return None
        except Exception as e:
            logger.warning(f"Error processing file {fits_fp}: {str(e)}")
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
                    'ra': hdu[1].header['ra_obj'],
                    'dec': hdu[1].header['dec_obj']
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

    def _create_catalog_from_failed(self, failed_downloads):
        """Create a catalog from failed downloads for TGLC"""
        if failed_downloads.empty:
            return None
            
        rows = []
        if isinstance(failed_downloads, Table):
            failed_downloads = failed_downloads.to_pandas()
        for _, row in failed_downloads.iterrows():
            # Extract GAIADR3_ID from file_id
            parts = row['file_id'].split('_')
            if len(parts) >= 2:
                gaia_id = int(parts[1])
                
                # Extract cam, ccd, and fp values from file_path
                path_parts = row['file_path'].split('/')
                if len(path_parts) >= 7:  # Ensure path has enough parts
                    cam_ccd_part = path_parts[-6]
                    cam = int(cam_ccd_part.split('-')[0].replace('cam', ''))
                    ccd = int(cam_ccd_part.split('-')[1].replace('ccd', ''))
                    fp_values = path_parts[-5:-1]  # Extract fp1, fp2, fp3, fp4
                    if len(fp_values) == 4:
                        rows.append([gaia_id, cam, ccd] + [int(fp) for fp in fp_values])
        
        if not rows:
            return None
            
        return Table(rows=rows, names=self.catalog_column_names)

    # Add to TGLC_Downloader class
    def direct_processing_fn(
            self,
            catalog_row : Row,
            del_fits : bool = True
    ) -> dict:
        ''' 
        Process a single light curve file for direct downloads
        
        Parameters
        ----------
        row: astropy Row, a single row from the sector catalog
        
        Returns
        ------- 
        lightcurve: dict, light curve in the standard format
        '''
        
        # For direct downloads, we need to find the file by searching the fits_dir
        # for files matching the GAIADR3_ID
        gaia_id = catalog_row['GAIADR3_ID']
        
        # Search for the file in the fits_dir
        fits_files = []
        for root, _, files in os.walk(self.fits_dir):
            for file in files:
                if file.endswith('.fits') and str(gaia_id) in file:
                    fits_files.append(os.path.join(root, file))
        
        if not fits_files:
            logger.warning(f"No fits file found for GAIADR3_ID {gaia_id}")
            return None
        
        # Use the first matching file
        fits_fp = fits_files[0]
        
        try:
            with fits.open(fits_fp, mode='readonly', memmap=True) as hdu:
                entry = {
                    'object_id': catalog_row.get('TIC_ID', None),
                    'GAIADR3_ID': gaia_id,
                    'time': hdu[1].data['time'],
                    'psf_flux': hdu[1].data['psf_flux'],
                    'psf_flux_err': hdu[1].header['psf_err'],
                    'aper_flux': hdu[1].data['aperture_flux'],
                    'aper_flux_err': hdu[1].header['aper_err'],
                    'tess_flags': hdu[1].data['TESS_flags'],
                    'tglc_flags': hdu[1].data['TGLC_flags'],
                    'ra': hdu[1].header['ra_obj'],
                    'dec': hdu[1].header['dec_obj']
                }
                if del_fits:
                    os.remove(fits_fp)
                    try:
                        os.rmdir(os.path.dirname(fits_fp))
                    except:
                        pass
                return entry
            
        except FileNotFoundError:
            logger.warning(f"File not found: {fits_fp}")
            return None
        except Exception as e:
            logger.warning(f"Error processing file {fits_fp}: {str(e)}")
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
                    'ra': hdu[0].header['ra_obj'],
                    'dec': hdu[0].header['dec_obj'],
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

    def _create_catalog_from_failed(self, failed_downloads):
        """Create a catalog from failed downloads for QLP"""
        if failed_downloads.empty:
            return None
            
        rows = []
        if isinstance(failed_downloads, Table):
            failed_downloads = failed_downloads.to_pandas()
        for _, row in failed_downloads.iterrows():
            # Extract TIC_ID from file_id
            parts = row['file_id'].split('_')
            if len(parts) >= 2:
                tic_id = int(parts[1])
                
                # Extract fp values from file_path
                path_parts = row['file_path'].split('/')
                if len(path_parts) >= 5:  # Ensure path has enough parts
                    fp_values = path_parts[-5:-1]  # Extract fp1, fp2, fp3, fp4
                    if len(fp_values) == 4:
                        rows.append([tic_id] + [int(fp) for fp in fp_values])
        
        if not rows:
            return None
            
        return Table(rows=rows, names=self.catalog_column_names)

    # Add to QLP_Downloader class
    def direct_processing_fn(
            self,
            catalog_row : Row,
            del_fits : bool = True
    ) -> dict:
        ''' 
        Process a single light curve file for direct downloads
        
        Parameters
        ----------
        row: astropy Row, a single row from the sector catalog
        
        Returns
        ------- 
        lightcurve: dict, light curve in the standard format
        '''
        
        # For direct downloads, we need to find the file by searching the fits_dir
        # for files matching the TIC_ID
        tic_id = catalog_row['TIC_ID']
        
        # Search for the file in the fits_dir
        fits_files = []
        for root, _, files in os.walk(self.fits_dir):
            for file in files:
                if file.endswith('.fits') and str(tic_id) in file:
                    fits_files.append(os.path.join(root, file))
        
        if not fits_files:
            logger.warning(f"No fits file found for TIC_ID {tic_id}")
            return None
        
        # Use the first matching file
        fits_fp = fits_files[0]
        
        try:
            with fits.open(fits_fp, mode='readonly', memmap=True) as hdu:
                # see docs @ https://archive.stsci.edu/hlsps/qlp/hlsp_qlp_tess_ffi_all_tess_v1_data-prod-desc.pdf
                entry = {
                    'object_id': tic_id,
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
                    'ra': hdu[0].header['ra_obj'],
                    'dec': hdu[0].header['dec_obj'],
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
            logger.warning(f"File not found: {fits_fp}")
            return None
        except Exception as e:
            logger.warning(f"Error processing file {fits_fp}: {str(e)}")
            return None

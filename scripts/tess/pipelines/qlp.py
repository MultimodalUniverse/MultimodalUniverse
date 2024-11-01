from base_downloader import TESS_Downloader
import os
import datasets
from datasets import Features, Value, Sequence
import numpy as np 
import itertools
import h5py
import astropy.io as fits 
from astropy.table.row import Row
from astropy.io import fits

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

class QLP_Config(datasets.BuilderConfig):
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

class QLP(datasets.GeneratorBasedBuilder):
    VERSION = "0.0.1"
    DEFAULT_CONFIG_NAME = "all"

    BUILDER_CONFIG_CLASS = QLP_Config
        
    @classmethod
    def _info(self):
        features = {"lightcurve" : Sequence({
            'time': Value(dtype="float32"),
            'sap_flux': Value(dtype="float32"),
            'kspsap_flux':  Value(dtype="float32"),
            'kspsap_flux_err':  Value(dtype="float32"),
            'quality':  Value(dtype="float32"), 
            'orbitid':  Value(dtype="float32"),
            'sap_x':  Value(dtype="float32"),
            'sap_y':  Value(dtype="float32"),
            'sap_bkg':  Value(dtype="float32"),
            'sap_bkg_err':  Value(dtype="float32"),
            'kspsap_flux_sml':  Value(dtype="float32"),
            'kspsap_flux_lag':  Value(dtype="float32"),
            }),
            'RA':  Value(dtype="float32"),
            'DEC':  Value(dtype="float32"),
            'TIC_ID': Value(dtype="string"),
            'tess_mag': Value(dtype="float32"),
            'radius':  Value(dtype="float32"),
            'teff': Value(dtype="float32"),
            'logg': Value(dtype="float32"),
            'mh': Value(dtype="float32")
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
                        'sap_flux': data["sap_flux"][i],
                        'kspsap_flux':  data["kspsap_flux"][i],
                        'kspsap_flux_err':  data["kspsap_flux_err"][i],
                        'quality':  data["quality"][i], 
                        'orbitid':   data["orbitid"][i],
                        'sap_x':  data["sap_x"][i],
                        'sap_y':  data["sap_y"][i],
                        'sap_bkg':  data["sap_bkg"][i],
                        'sap_bkg_err':  data["sap_bkg_err"][i],
                        'kspsap_flux_sml':  data["kspsap_flux_sml"][i],
                        'kspsap_flux_lag':  data["kspsap_flux_lag"][i],
                        },
                        'RA':  data["RA"][i],
                        'DEC':  data["DEC"][i],
                        'TIC_ID': data["TIC_ID"][i],
                        'tess_mag': data["tess_mag"][i],
                        'radius':  data["radius"][i],
                        'teff': data["teff"][i],
                        'logg': data["logg"][i],
                        'mh': data["mh"][i]
                    }
                    yield str(data["TIC_ID"][i]), example 

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
        path = f'{catalog_row["fp1"]}/{catalog_row["fp2"]}/{catalog_row["fp3"]}/{catalog_row["fp4"]}/hlsp_qlp_tess_ffi_{self.sector_str}-{f'{catalog_row["TIC_ID"]:016d}'}_tess_v01_llc.fits'
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

                return {
                    'TIC_ID': catalog_row['TIC_ID'],
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
            
        except FileNotFoundError:
            print(f"File not found: {fits_fp}")
            # Not sure why some files are not found in the tests
            return None
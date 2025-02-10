from base_downloader import TESS_Downloader
import datasets
from datasets import Features, Value, Sequence
from datasets.data_files import DataFilesPatternsDict
import numpy as np 
import itertools
import h5py
from astropy.table.row import Row
import os 
from astropy.io import fits

_CITATION = """\
@ARTICLE{2023AJ....165...71H,
       author = {{Han}, Te and {Brandt}, Timothy D.},
        title = "{TESS-Gaia Light Curve: A PSF-based TESS FFI Light-curve Product}",
      journal = {\aj},
     keywords = {Light curves, Astronomy software, Astronomy databases, Exoplanet astronomy, Variable stars, Eclipsing binary stars, 918, 1855, 83, 486, 1761, 444, Astrophysics - Instrumentation and Methods for Astrophysics, Astrophysics - Earth and Planetary Astrophysics, Astrophysics - Solar and Stellar Astrophysics},
         year = 2023,
        month = feb,
       volume = {165},
       number = {2},
          eid = {71},
        pages = {71},
          doi = {10.3847/1538-3881/acaaa7},
archivePrefix = {arXiv},
       eprint = {2301.03704},
 primaryClass = {astro-ph.IM},
       adsurl = {https://ui.adsabs.harvard.edu/abs/2023AJ....165...71H},
      adsnote = {Provided by the SAO/NASA Astrophysics Data System}
}
"""

_DESCRIPTION = """
TESS-Gaia Light Curve: Light curves from TESS FFI images using GAIA based priors.
"""
_HOMEPAGE = "https://archive.stsci.edu/hlsp/tglc"
_LICENSE = "CC BY 4.0"
_VERSION = "0.0.1"


class TGLC(datasets.GeneratorBasedBuilder):
    VERSION = "0.0.1"
    DEFAULT_CONFIG_NAME = "all"

    BUILDER_CONFIGS = [
    datasets.BuilderConfig(
        name="all",
        version=_VERSION,
        data_files=DataFilesPatternsDict.from_patterns(
            {"train": ["./TGLC/healpix=*/*.hdf5"]} 
        ),
        description="TGLC light curves",
        )
    ]

    @classmethod
    def _info(self):
        features = {"lightcurve" : Sequence({
            'time': Value(dtype="float32"),
            'psf_flux': Value(dtype="float32"),
            'aper_flux':  Value(dtype="float32"),
            'tess_flags':  Value(dtype="float32"),
            'tglc_flags':  Value(dtype="float32"),
            
        }),
            'RA':  Value(dtype="float32"),
            'DEC':  Value(dtype="float32"),
            'object_id': Value(dtype="string"),
            'GAIADR3_ID': Value(dtype="string"),
            'aper_flux_err':  Value(dtype="float32"),
            'psf_flux_err': Value(dtype="float32"),
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
                    keys = data["object_id"][:]
                
                # Preparing an index for fast searching through the catalog
                sort_index = np.argsort(data["object_id"][:])
                sorted_ids = data["object_id"][:][sort_index]
               
                for k in keys:
                    # Extract the indices of requested ids in the catalog
                    i = sort_index[np.searchsorted(sorted_ids, k)]
                    example = {
                        "lightcurve" : {
                            'time': data["time"][i],
                            'psf_flux': data["psf_flux"][i],
                            'aper_flux':  data["aper_flux"][i],
                            'tess_flags':  data["tess_flags"][i],
                            'tglc_flags':  data["tglc_flags"][i]
                        }, 
                        'RA':  data["RA"][i],
                        'DEC':  data["DEC"][i],
                        'object_id': data["object_id"][i],
                        'GAIADR3_ID': data["GAIADR3_ID"][i],
                        'psf_flux_err': data["psf_flux_err"][i],
                        'aper_flux_err':  data["aper_flux_err"][i]
                    }
                    yield str(data["object_id"][i]), example

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
                    os.rmdir(os.path.dirname(fits_fp))
                return entry
            
        except FileNotFoundError:
            print(f"File not found: {fits_fp}")
            return None
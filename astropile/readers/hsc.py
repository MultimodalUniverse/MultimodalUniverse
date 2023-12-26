import os
import numpy as np
import h5py
from astropy.table import Table
from .reader import BaseReader
from typing import List, Any, Dict

class HSCDUDReader(BaseReader):
    """
    A reader for HSC Deep/Ultra Deep cutouts data.
    """

    _bands = ['G', 'R', 'I', 'Z', 'Y']

    def __init__(self, 
                 dataset_path: str, 
                 image_size: int = 144):
        super().__init__(dataset_path)
        self._catalog = Table.read(os.path.join(self.dataset_path, 'pdr3_dud_22.5.fits'))
        self._data = h5py.File(os.path.join(self.dataset_path, 'cutouts_pdr3_dud_rev_coadd.hdf'), 'r')
        self._image_size = image_size
    
    @property
    def catalog(self) -> Table:
        return self._catalog

    def get_examples(self, ids: List[Any]) -> Dict[str, Any]:

        # Preparing an index for fast searching through the catalog
        sort_index = np.argsort(self._catalog['object_id'])
        sorted_ids = self._catalog['object_id'][sort_index]

        # count how many times we run into problems with the images
        n_problems = 0

        # Loop over the indices and yield the requested data
        for i, id in enumerate(ids):
            # Extract the indices of requested ids in the catalog 
            idx = sort_index[np.searchsorted(sorted_ids, id)]
            row = self._catalog[idx]
            key = str(row['object_id'])
            hdu = self._data[key]

            # Get the smallest shape among all images
            s_x = min([hdu[f'HSC-{band}']['HDU0']['DATA'].shape[0] for band in self._bands])
            s_y = min([hdu[f'HSC-{band}']['HDU0']['DATA'].shape[1] for band in self._bands])

            # Raise a warning if one of the images has a different shape than 'smallest_shape'
            for band in self._bands:
                if hdu[f'HSC-{band}']['HDU0']['DATA'].shape != (s_x, s_y):
                    print(f"Warning: The image for object {id} has a different shape depending on the band. It's the {n_problems+1}th time this happens.")
                    n_problems += 1
                    break

            # Crop the images to the smallest shape
            image = np.stack([
                hdu[f'HSC-{band}']['HDU0']['DATA'][:s_x, :s_y].astype(np.float32)
                for band in self._bands
            ], axis=0)
            
            # Cutout the center of the image to desired size
            s = image.shape
            center_x = s[1] // 2
            start_x = center_x - self._image_size // 2
            center_y = s[2] // 2
            start_y = center_y - self._image_size // 2
            image = image[:, 
                          start_x:start_x+self._image_size, 
                          start_y:start_y+self._image_size]
            assert image.shape == (5, self._image_size, self._image_size), ("There was an error in reshaping the image to desired size", image.shape, s )

            # Initialize the example with image data
            example = {
                f'image_{band.lower()}': image[k] for k, band in enumerate(self._bands)
            }
            # Add additional catalog information
            for band in self._bands:
                band = band.lower()
                example[f'a_{band}'] = row[f'a_{band}']
                example[f'mag_{band}'] = row[f'{band}_cmodel_mag']
                example[f'psf_shape11_{band}'] = row[f'{band}_sdssshape_psf_shape11']
                example[f'psf_shape12_{band}'] = row[f'{band}_sdssshape_psf_shape12']
                example[f'psf_shape22_{band}'] = row[f'{band}_sdssshape_psf_shape22']
                # and so on...

            # Checking that we are retriving the correct data
            assert (row['object_id'] == ids[i]), ("There was an indexing error when reading the hsc cutouts", (row['object_id'], ids[i]))

            yield f'hsc_{ids[i]}', example
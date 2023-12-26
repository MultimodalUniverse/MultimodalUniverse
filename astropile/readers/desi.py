import os
import numpy as np
from astropy.table import Table
import h5py
from .reader import BaseReader
from typing import List, Any, Dict


class DESIReader(BaseReader):
    """
    A reader for DESI data.
    """
    def __init__(self, dataset_path: str):
        super().__init__(dataset_path)
        self._catalog = Table.read(os.path.join(self.dataset_path, 'desi_catalog.fits'))
        self._data = h5py.File(os.path.join(self.dataset_path, 'desi_spectra.hdf5'), 'r')
    
    @property
    def catalog(self) -> Table:
        return self._catalog

    def get_examples(self, ids: List[Any]) -> Dict[str, Any]:

        # Preparing an index for fast searching through the catalog
        sort_index = np.argsort(self._catalog['TARGETID'])

        # Loop over the indices and yield the requested data
        for i, id in enumerate(ids):
            # Extract the indices of requested ids in the catalog 
            idx = sort_index[np.searchsorted(self._catalog['TARGETID'], id, sorter=sort_index)]

            example = {
                'spectrum': self._data['flux'][idx],
                'z': self._catalog['Z'][idx],
                'ebv': self._catalog['EBV'][idx],
            }

            # Checking that we are retriving the correct data
            assert (self._catalog['TARGETID'][idx] == ids[i]) & (self._data['target_ids'][idx] == ids[i]) , ("There was an error in reading the requested spectra from the file", (self._catalog['TARGETID'][idx], self._data['target_ids'][idx] , ids[i]))

            yield example

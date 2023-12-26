from astropy.table import Table
from typing import List, Any, Dict


class BaseReader:    
    def __init__(self, dataset_path: str):
        """
        Initialize the reader.

        Args:
            dataset_path (str): The path to the dataset.
        """
        self.dataset_path = dataset_path

    @property
    def get_catalog(self) -> Table:
        """
        Get the catalog for the sample.

        Returns:
            Any: The catalog for the sample.
        """
        # Implement the logic to get the catalog for the sample
        raise NotImplementedError

    def get_examples(self, ids: List[Any]) -> Dict[str, Any]:
        """
        Create a generator function returnin the examples corresponding to 
        the provided ids.

        Args:
            ids (List[Any]): The ids of the data to be read.

        Yields:
            Dict[str, Any]: The example for that id.
        """
        raise NotImplementedError
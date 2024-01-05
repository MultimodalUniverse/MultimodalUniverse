import os
import datasets
from dataclasses import dataclass
from typing import List, Optional
import itertools
from datasets import DatasetBuilder, Dataset
from astropy.table import Table, hstack
from astropy.coordinates import SkyCoord
from astropy import units as u
from typing import List
from functools import partial
import pyarrow as pa


logger = datasets.utils.logging.get_logger(__name__)

@dataclass
class JoinedAstroPileDatasetConfig(datasets.BuilderConfig):
    """ BuilderConfig for JoinedAstroPileDataset """

    left: DatasetBuilder
    right: DatasetBuilder
    matching_fn: List[List[str], callable]
    batch_size: int = 10_000
    columns: Optional[List[str]] = None
    features: Optional[datasets.Features] = None


class JoinedAstroPileDataset(datasets.ArrowBasedBuilder):
    """ Class implementing a joining operation on two datasets that follow the 
    AstroPile conventions.
    """

    BUILDER_CONFIG_CLASS = JoinedAstroPileDatasetConfig

    def _info(self):
        # Merge features from both datasets, adding a suffix to indicate which dataset this
        # feature comes from
        left_features = {k+'_left': v for k, v in self.config.left.info.features.items()}
        right_features = {k+'_right': v for k, v in self.config.right.info.features.items()}
        # TODO: write smarter code that allows for subselecting features based on columns
        features = datasets.Features({**left_features, **right_features})
        return datasets.DatasetInfo(features=features)

    def _split_generators(self, dl_manager):
        """We handle string, list and dicts in datafiles"""
        if not self.config.data_files:
            raise ValueError(f"At least one data file must be specified, but got data_files={self.config.data_files}")
        data_files = dl_manager.download_and_extract(self.config.data_files)
        if isinstance(data_files, (str, list, tuple)):
            files = data_files
            if isinstance(files, str):
                files = [files]
            # Use `dl_manager.iter_files` to skip hidden files in an extracted archive
            files = [dl_manager.iter_files(file) for file in files]
            return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={"files": files})]
        splits = []
        for split_name, files in data_files.items():
            if isinstance(files, str):
                files = [files]
            # Use `dl_manager.iter_files` to skip hidden files in an extracted archive
            files = [dl_manager.iter_files(file) for file in files]
            # Infer features if they are stored in the arrow schema
            if self.info.features is None:
                for file in itertools.chain.from_iterable(files):
                    with open(file, "rb") as f:
                        self.info.features = datasets.Features.from_arrow_schema(pq.read_schema(f))
                    break
            splits.append(datasets.SplitGenerator(name=split_name, gen_kwargs={"files": files}))
        if self.config.columns is not None and set(self.config.columns) != set(self.info.features):
            self.info.features = datasets.Features(
                {col: feat for col, feat in self.info.features.items() if col in self.config.columns}
            )
        return splits

    def _generate_tables(self, files):
        if self.config.features is not None and self.config.columns is not None:
            if sorted(field.name for field in self.info.features.arrow_schema) != sorted(self.config.columns):
                raise ValueError(
                    f"Tried to load parquet data with columns '{self.config.columns}' with mismatching features '{self.info.features}'"
                )
        for file_idx, file in enumerate(itertools.chain.from_iterable(files)):
            with open(file, "rb") as f:
                parquet_file = pq.ParquetFile(f)
                try:
                    for batch_idx, record_batch in enumerate(
                        parquet_file.iter_batches(batch_size=self.config.batch_size, columns=self.config.columns)
                    ):
                        pa_table = pa.Table.from_batches([record_batch])
                        # Uncomment for debugging (will print the Arrow table size and elements)
                        # logger.warning(f"pa_table: {pa_table} num rows: {pa_table.num_rows}")
                        # logger.warning('\n'.join(str(pa_table.slice(i, 1).to_pydict()) for i in range(pa_table.num_rows)))
                        yield f"{file_idx}_{batch_idx}", self._cast_table(pa_table)
                except ValueError as e:
                    logger.error(f"Failed to read file '{file}' with error {type(e)}: {e}")
                    raise











import argparse
import glob
import os
import os.path
import socket
import warnings
from dataclasses import dataclass
from typing import Any, List

import h5py as h5
import numpy as np
from astropy.table import Table
from astropy.units import UnitsWarning
from dask_hpc_runner.slurm import initialize
from distributed import Client, Lock, performance_report, worker_client
from distributed.scheduler import logger
from processor import (
    BrickProcessor,
    CatalogProcessor,
    HealpixProcessor,
    ObjectInformation,
)

# Ignore some astropy warnings
warnings.simplefilter("ignore", UnitsWarning)


@dataclass
class ProcessingOutput:
    success: bool
    healpix: str
    brickname: str


def write_object_data(obj: ObjectInformation, file: h5.File):
    group_name = str(obj.id)
    group = file.create_group(group_name)
    # Write all data of the object as a HDF5 dataset
    for key in [
        "ra",
        "dec",
        "type",
        "ebv",
        "flux_g",
        "flux_r",
        "flux_i",
        "flux_z",
        "flux_w1",
        "flux_w2",
        "flux_w3",
        "flux_w4",
    ]:
        group.create_dataset(key, data=np.array([obj.__getattribute__(key)]))
    for key in [
        "image",
        "invvar",
        "bit_mask",
        "image_model",
        "object_mask",
    ]:
        group.create_dataset(key, data=obj.__getattribute__(key), compression="gzip")
    for key, val in obj.catalog.items():
        group.create_dataset(f"catalog_{key}".lower(), data=val, compression="gzip")


def process_catalogs(
    client: Client, catalogs: List[str], data_dir: str, output_dir: str
) -> List[Any]:
    futures = []
    for catalog in catalogs:
        catalog_processor = CatalogProcessor(catalog)
        for healpix in catalog_processor.generate_healpix():
            healpix_processor = HealpixProcessor(healpix)
            healpix_id = str(healpix_processor.id)
            healpix_dir = os.path.join(output_dir, healpix_id)
            os.makedirs(healpix_dir, exist_ok=True)
            for brick in healpix_processor.generate_bricks():
                future = client.submit(process_brick, brick, data_dir)
                futures.append(future)
    client.gather(futures, errors="skip")
    return [future.result() for future in futures]


def process_brick(brick: Table, data_dir: str, output_dir: str):
    """Multiprocessing task that handles a brick.
    It writes in a HDF5 file associated to the healpix
    all information about the object within the brick.

    """
    healpix_id = str(brick["HEALPIX"][0])
    brick_name = str(brick["BRICKNAME"][0])
    output_filename = os.path.join(output_dir, f"{healpix_id}.hdf5")
    # Workaround for writing to HDF5 from multiple processes
    with Lock(name=healpix_id):
        success = True
        try:
            processor = BrickProcessor(data_dir, brick)
            with h5.File(output_filename, "a") as file:
                for obj in processor.generate_objects():
                    write_object_data(obj, file)
        except Exception as e:
            logger.error(f"Failed to process birck {brick_name}: {e}")
            success = False
            raise e
        finally:
            return ProcessingOutput(success, healpix_id, brick_name)


def get_failures(processes: List[ProcessingOutput]) -> List[ProcessingOutput]:
    return [proc for proc in processes if proc.success is False]


def get_catalog_filenames(data_dir: str) -> List[str]:
    """Check raw data from legacy survey are present.
    Returns the list of catalog filenames.

    """
    sweep_catalog_dir = os.path.join(data_dir, "dr10", "south", "sweep", "10.1")
    assert os.path.isdir(
        sweep_catalog_dir
    ), f"Sweep catalogs directory {sweep_catalog_dir} is not a directory."
    sweep_catalogs = glob.glob(os.path.join(sweep_catalog_dir, "*.fits"))
    assert len(sweep_catalogs), f"No catalog found in {sweep_catalog_dir}"
    return sweep_catalogs


def check_existing_files(output_dir: str) -> bool:
    """Check if HDF5 files already exist in the output directory."""
    hdf5_files = glob.glob(os.path.join(output_dir, "*", "*.hdf5"))
    return len(hdf5_files) > 0


def main(data_dir: str, output_dir: str):
    # Create dask client for multiprocessing
    with initialize() as runner:
        with Client(runner) as client:
            # Check raw data from legacy survey are present
            sweep_catalogs = get_catalog_filenames(data_dir)
            logger.info(f"Found {len(sweep_catalogs)} catalogs.")

            existing_hdf5_outputs = check_existing_files(output_dir)
            if existing_hdf5_outputs:
                logger.info(
                    f"HDF5 files already present in {output_dir}. Object data will not be override."
                )

            logger.info(f"Successfully created client: {client}")
            host = client.run_on_scheduler(socket.gethostname)
            port = client.scheduler_info()["services"]["dashboard"]
            logger.info(f"{client.dashboard_link}")
            logger.info(
                f"Remote access to dashboard: ssh -N -L {port}:localhost:{port} {host}"
            )
            # Log dask computation for offline use
            with performance_report(filename="dask-report.html"):
                brick_results = process_catalogs(
                    client, sweep_catalogs, data_dir, output_dir
                )
            failures = get_failures(brick_results)
            logger.warning(f"Failed jobs {len(failures)}/{len(brick_results)}")
            for job in failures:
                logger.error(f"Failed healpix {job.healpix} brick {job.brickname}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        "Legacy survey dataset creator",
        description="Process original legacy survey files to create a dataset.",
    )
    parser.add_argument(
        "data_dir", type=str, help="Path to the local copy of the legacy survey data"
    )
    parser.add_argument(
        "output_dir",
        type=str,
        help="Path to the output directory where dataset will be stored",
    )
    args = parser.parse_args()
    data_dir = args.data_dir
    output_dir = args.output_dir

    main(data_dir, output_dir)

import argparse
import glob
import os
import os.path
import time
import traceback
import warnings
from typing import List

import h5py as h5
import numpy as np
from astropy.table import Table
from astropy.units import UnitsWarning
from dask.distributed import (
    Client,
    Future,
    LocalCluster,
    Lock,
    Queue,
    fire_and_forget,
    get_client,
    performance_report,
)
from processor import (
    BrickProcessor,
    CatalogProcessor,
    HealpixProcessor,
    ObjectInformation,
)

# Ignore some astropy warnings
warnings.simplefilter("ignore", UnitsWarning)


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


def process_catalog(filename: str):
    """Multiprocessing task that handles a catalog file.
    It dispatches new tasks to process each healpix independently.

    """
    client = get_client()
    processor = CatalogProcessor(filename)
    for healpix in processor.generate_healpix():
        future = client.submit(process_healpix, healpix)
        fire_and_forget(future)
        time.sleep(1)


def process_healpix(healpix: Table):
    """Multiprocessing task that handles a healpix.
    It dispatches new tasks to process each brick independently.

    """
    client = get_client()
    processor = HealpixProcessor(healpix)
    healpix_id = str(processor.id)
    healpix_dir = os.path.join(output_dir, healpix_id)
    os.makedirs(healpix_dir, exist_ok=True)
    for brick in processor.generate_bricks():
        future = client.submit(process_brick, brick, healpix_dir)
        brick_future_queue.put(future)


def process_brick(brick: Table, output_dir: str):
    """Multiprocessing task that handles a brick.
    It writes in a HDF5 file associated to the healpix
    all information about the object within the brick.

    """
    client = get_client()
    processor = BrickProcessor(data_dir, brick)
    healpix_id = str(brick["HEALPIX"][0])
    lock = Lock(name=healpix_id, client=client)
    output_filename = os.path.join(output_dir, f"{healpix_id}.hdf5")
    # Workaround for writing to HDF5 from multiple processes
    with lock:
        with h5.File(output_filename, "a") as file:
            for obj in processor.generate_objects():
                write_object_data(obj, file)


def get_futures(future_queue: Queue) -> List[Future]:
    futures = []
    while future_queue.qsize() > 0:
        future = future_queue.get()
        futures.append(future)
    return futures


def get_failures(futures: List[Future]) -> List[Future]:
    failures = []
    for future in futures:
        if future.status == "error":
            failures.append(future)

    # If limited error print them
    if len(failures) <= 10:
        for failure in failures:
            tb = traceback.format_tb(failure.traceback())
            print(tb)

    return failures


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
    parser.add_argument(
        "--n_proc",
        type=int,
        default=1,
        help="Number of processes to use for parallelism",
    )
    args = parser.parse_args()
    data_dir = args.data_dir
    output_dir = args.output_dir
    n_proc = args.n_proc

    # Check raw data from legacy survey are present
    sweep_catalogs = get_catalog_filenames(data_dir)
    print(f"Found {len(sweep_catalogs)} catalogs.")

    existing_hdf5_outputs = check_existing_files(output_dir)
    if existing_hdf5_outputs:
        print(
            f"HDF5 files already present in {output_dir}. Object data will not be override."
        )

    # Create dask client for multiprocessing
    cluster = LocalCluster(
        n_workers=n_proc,
        processes=True,
        memory_limit="auto",
    )
    client = Client(cluster)
    print(f"Successfully created client with {n_proc} workers: {client}")
    print(f"{client.dashboard_link}")
    brick_future_queue = Queue(client=client)
    # Log dask computation for offline use
    with performance_report(filename="dask-report.html"):
        futures = client.map(process_catalog, sweep_catalogs)
        client.gather(futures)
        del futures
        brick_future_queue.close()
    brick_futures = get_futures(brick_future_queue)
    failures = get_failures(brick_futures)
    print(f"Failed jobs {len(failures)}/{len(brick_futures)}")
    client.shutdown()

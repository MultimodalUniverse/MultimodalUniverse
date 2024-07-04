import argparse
import glob
import os.path
import time
import warnings
import traceback
from typing import List

from astropy.table import Table
from astropy.units import UnitsWarning
from dask.distributed import (
    Client,
    LocalCluster,
    Queue,
    fire_and_forget,
    get_client,
    Future,
)
from processor import BrickProcessor, CatalogProcessor, HealpixProcessor

warnings.simplefilter("ignore", UnitsWarning)


def process_catalog(filename: str):
    client = get_client()
    processor = CatalogProcessor(filename)
    for healpix in processor.generate_healpix():
        future = client.submit(process_healpix, healpix)
        fire_and_forget(future)
        time.sleep(1)


def process_healpix(healpix: Table):
    client = get_client()
    processor = HealpixProcessor(healpix)
    for brick in processor.generate_bricks():
        future = client.submit(process_brick, brick)
        brick_future_queue.put(future)


def process_brick(brick: Table):
    processor = BrickProcessor(output_dir, brick)
    for obj in processor.generate_objects():
        # Write in HDF5
        pass


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
    sweep_catalog_dir = os.path.join(data_dir, "dr10", "south", "sweep", "10.1")
    assert os.path.isdir(
        sweep_catalog_dir
    ), f"Sweep catalogs directory {sweep_catalog_dir} is not a directory."
    sweep_catalogs = glob.glob(os.path.join(sweep_catalog_dir, "*.fits"))
    assert len(sweep_catalogs), f"No catalog found in {sweep_catalog_dir}"
    print(f"Found {len(sweep_catalogs)} catalogs.")

    n_proc = args.n_proc
    cluster = LocalCluster(
        n_workers=n_proc,
        processes=True,
        memory_limit="auto",
    )
    client = Client(cluster)
    brick_future_queue = Queue(client=client)
    print(f"Successfully created client with {n_proc} workers: {client}")
    print(f"{client.dashboard_link}")
    futures = client.map(process_catalog, sweep_catalogs)
    client.gather(futures)
    del futures
    brick_future_queue.close()
    brick_futures = get_futures(brick_future_queue)
    failures = get_failures(brick_futures)
    print(f"Failed jobs {len(failures)}/{len(brick_futures)}")
    client.shutdown()

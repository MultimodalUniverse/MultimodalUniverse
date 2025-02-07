N_PROCESSES = 4 
SECTOR = 23

import os
import sys 
import argparse

sys.path.append(".")

import pytest
from __init__ import PIPELINES


def test_pipeline(pipeline):
    Downloader = PIPELINES[pipeline]["Downloader"]

    DOWNLOADER = Downloader(
        sector=SECTOR,
        data_path = PIPELINES[pipeline]["DATA_PATH"], 
        hdf5_output_dir = PIPELINES[pipeline]["DATA_PATH"],
        fits_dir = os.path.join(PIPELINES[pipeline]["DATA_PATH"], 'fits'),
        n_processes = N_PROCESSES
    )
    DOWNLOADER.download_sector(
        tiny=True,
        show_progress=True,
        save_catalog=True,
        clean_up=False
    )

    pytest.main([f"test_{pipeline}.py"])
    return 

def main():
    options = list(PIPELINES.keys()) + ["all"]

    parser = argparse.ArgumentParser(description="This script tests the minidownloads for each pipeline.")
    parser.add_argument('--pipeline', type=str, help=f"TESS pipeline to download. Options are {options}.")

    args = parser.parse_args() 

    if args.pipeline not in options:
        raise ValueError(f"Invalid pipeline {args.pipeline}. Options are {options}")

    if args.pipeline != "all":
        test_pipeline(args.pipeline)

    else:
        for pipeline in PIPELINES.keys():
            test_pipeline(pipeline)

if __name__ == '__main__':
    main()
import argparse
import os
import urllib.request
from functools import partial

from tqdm.contrib.concurrent import process_map


def _download_file(f, args):
    f = f.strip()
    filename = f.split("wise-allwise.parquet/")[-1]
    filedir = "/".join(filename.split("/")[:-1])
    os.makedirs(f"{args.output_dir}/{filedir}", exist_ok=True)
    savename = f"{args.output_dir}/{filename}"
    if not os.path.exists(savename):
        urllib.request.urlretrieve(f, savename)


def main(args):
    if not os.path.exists("file_list.txt"):
        # getting file list
        os.system(
            "wget https://irsa.ipac.caltech.edu/data/download/parquet/wise/allwise/healpix_k5/wise-allwise-download-irsa-wget.sh -O _file_list.txt"
        )

        out_lines = []

        with open("_file_list.txt", "r") as f:
            lines = f.readlines()
            for line in lines:
                if "wise-allwise.parquet" in line:
                    out_lines.append("https" + line.split("https")[1].strip())

        with open("file_list.txt", "w") as f:
            f.write("\n".join(out_lines))

        os.remove("_file_list.txt")

    os.makedirs(args.output_dir, exist_ok=True)

    with open("file_list.txt") as f:
        files = f.readlines()

    if args.tiny:
        files = files[:3]

    _download = partial(_download_file, args=args)

    process_map(_download, files, max_workers=16, chunksize=1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--tiny",
        help="download a single file only",
        action="store_true",
    )
    parser.add_argument(
        "--output_dir",
        help="output directory",
        default=".",
    )
    args = parser.parse_args()
    main(args)

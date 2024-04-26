import argparse
from functools import partial
import os
import urllib.request

from tqdm.contrib.concurrent import process_map


def _download_file(f, args):
    f = f.strip()
    savename = f"{args.output_dir}/{f.split('/')[-1]}"
    if not os.path.exists(savename):
        urllib.request.urlretrieve(f, savename)


def main(args):
    os.makedirs(args.output_dir, exist_ok=True)

    if not args.aria2:
        with open("file_list.txt") as f:
            files = f.readlines()

        if args.tiny:
            files = files[:3]

        _download = partial(_download_file, args=args)

        process_map(_download, files, max_workers=16, chunksize=1)

    else:
        if args.tiny:
            with open("file_list.txt") as f:
                files = f.readlines()[:3]
            files = " ".join([f'"{f.strip()}"' for f in files])

            os.system(f"aria2c -j8 -x8 -s8 -c -d {args.output_dir} -Z {files}")

        else:
            os.system(f"aria2c -j16 -x16 -s16 -c -i file_list.txt -d {args.output_dir}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--aria2", help="use aria2c for downloading", action="store_true"
    )
    parser.add_argument(
        "--tiny",
        help="download a single source and coeff file only",
        action="store_true",
    )
    parser.add_argument(
        "--output_dir",
        help="output directory",
        default=".",
    )
    args = parser.parse_args()
    main(args)

import argparse
import os
import urllib.request

from tqdm.contrib.concurrent import process_map


def _download_file(f):
    f = f.strip()
    savename = f"{args.output_dir}/{f.split('/')[-1]}"
    if not os.path.exists(savename):
        urllib.request.urlretrieve(f, savename)


def main(args):
    if not args.aria2:
        with open("source_file_list.txt") as f:
            source_files = f.readlines()

        with open("coeff_file_list.txt") as f:
            coeff_files = f.readlines()

        with open("rvs_file_list.txt") as f:
            rvs_files = f.readlines()

        if args.tiny:
            source_files = source_files[:1]
            coeff_files = coeff_files[:1]
            rvs_files = rvs_files[:1]

        files_flat = [*source_files, *coeff_files, *rvs_files]

        process_map(_download_file, files_flat, max_workers=16, chunksize=1)

    else:
        if args.tiny:
            with open("source_file_list.txt") as f:
                source_files = f.readline().strip()
            with open("coeff_file_list.txt") as f:
                coeff_files = f.readline().strip()
            with open("rvs_file_list.txt") as f:
                rvs_files = f.readline().strip()

            os.system(
                f'aria2c -j2 -x2 -s2 -c -d {args.output_dir} -Z "{source_files}" "{coeff_files}" "{rvs_files}"'
            )

        else:
            os.system(
                f"aria2c -j16 -x16 -s16 -c -i source_file_list.txt -d {args.output_dir}"
            )
            os.system(
                f"aria2c -j16 -x16 -s16 -c -i coeff_file_list.txt -d {args.output_dir}"
            )
            os.system(
                f"aria2c -j16 -x16 -s16 -c -i rvs_file_list.txt -d {args.output_dir}"
            )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--aria2", help="use aria2c for downloading", action="store_true"
    )
    parser.add_argument(
        "--tiny",
        help="download a single source,coeff,rvs file only",
        action="store_true",
    )
    parser.add_argument(
        "--output_dir",
        help="output directory",
        default=".",
    )
    args = parser.parse_args()
    main(args)

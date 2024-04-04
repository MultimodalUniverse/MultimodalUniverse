import urllib.request
import argparse
from tqdm.auto import tqdm
import os


def main(args):
    if not args.aria2:
        print("Downloading files using urllib, this will be slow...")

        with open("source_file_list.txt") as f:
            source_files = f.readlines()

        with open("coeff_file_list.txt") as f:
            coeff_files = f.readlines()

        if args.tiny:
            source_files = source_files[:1]
            coeff_files = coeff_files[:1]

        files_flat = [*source_files, *coeff_files]

        for f in tqdm(files_flat):
            f = f.strip()
            savename = f"{args.output_dir}/{f.split('/')[-1]}"
            if not os.path.exists(savename):
                urllib.request.urlretrieve(f, savename)
    else:
        if args.tiny:
            with open("source_file_list.txt") as f:
                source_files = f.readline().strip()
            with open("coeff_file_list.txt") as f:
                coeff_files = f.readline().strip()

            os.system(
                f'aria2c -j2 -x2 -s2 -c -d {args.output_dir} -Z "{source_files}" "{coeff_files}"'
            )

        else:
            os.system(
                f"aria2c -j16 -x16 -s16 -c -i source_file_list.txt -d {args.output_dir}"
            )
            os.system(
                f"aria2c -j16 -x16 -s16 -c -i coeff_file_list.txt -d {args.output_dir}"
            )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--aria2", help="use aria2c for downloading", action="store_true"
    )
    parser.add_argument(
        "--tiny", help="Download a single file only", action="store_true"
    )
    parser.add_argument(
        "--output_dir",
        help="Output directory",
        default="data",
    )
    args = parser.parse_args()
    main(args)

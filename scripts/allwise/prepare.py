import argparse
import os


def main(args):
    print("Getting file list...")
    os.system("python3 get_file_list.py")

    print("Downloading data...")
    os.system(
        f"python3 download_parts.py {'--tiny' if args.tiny else ''} --output_dir {args.data_dir}"
    )

    print("Healpixifying data...")
    os.system(
        f"python3 healpixify.py --nside {args.nside} --input_dir {args.data_dir} --output_dir {args.parquet_data_dir}"
    )

    print("Converting to HDF5...")
    os.system(
        f"python3 to_hdf5.py --data_dir {args.parquet_data_dir} --output_dir {args.output_dir}"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--nside", type=int, default=16)
    parser.add_argument("--tiny", action="store_true", default=False)
    parser.add_argument("--data_dir", type=str, default="_allwise")
    parser.add_argument("--parquet_data_dir", type=str, default="allwise_hp")
    parser.add_argument("--output_dir", type=str, default="allwise")
    args = parser.parse_args()
    main(args)

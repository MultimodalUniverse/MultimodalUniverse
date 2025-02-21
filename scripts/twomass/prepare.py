import argparse
import os


def main(args):
    print("Downloading data...")
    os.system(
        f"python3 download_parts.py {'--tiny' if args.tiny else ''} --output_dir {os.path.join(args.data_dir, 'psc')} {'--aria2' if args.aria2 else ''}"
    )

    print("Preparing partitioned parquet dataset...")
    os.system(
        f"python3 to_parquet.py --nside {args.nside} --data_dir {os.path.join(args.data_dir, 'psc')} --output_dir {os.path.join(args.parquet_data_dir, 'psc')} --file_prefix psc {'--tiny' if args.tiny else ''}"
    )

    print("Converting to HDF5...")
    os.system(
        f"python3 to_hdf5.py --data_dir {args.parquet_data_dir} --output_dir {args.output_dir}"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--tiny", action="store_true", help="Use tiny dataset")
    parser.add_argument("--aria2", action="store_true", help="Use aria2 to download")
    parser.add_argument("--nside", type=int, default=16, help="Healpix nside")
    parser.add_argument("--data_dir", type=str, default="_2mass", help="Data directory")
    parser.add_argument(
        "--parquet_data_dir", type=str, default="_2mass_pq", help="Data directory"
    )
    parser.add_argument(
        "--output_dir", type=str, default="2mass", help="Output directory"
    )
    args = parser.parse_args()
    main(args)

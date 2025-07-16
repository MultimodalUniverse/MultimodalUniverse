import argparse
import os


def main(args):
    print("Downloading data...")
    os.system(f"python3 download.py --output_dir {args.data_dir}")
    print("Processing data...")
    os.system(
        f"python3 process.py --input_dir {args.data_dir} --output_dir {args.output_dir} {'--tiny' if args.tiny else ''}"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--data_dir", type=str, default="data", help="Directory to download data to"
    )
    parser.add_argument(
        "--output_dir",
        type=str,
        default="data",
        help="Directory to save processed data",
    )
    parser.add_argument(
        "--tiny", action="store_true", help="Use tiny dataset", default=False
    )
    args = parser.parse_args()
    main(args)

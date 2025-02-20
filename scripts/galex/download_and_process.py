import argparse
import os


def main(args):
    print("Downloading...")
    os.system(
        f'python3 download_data.py --data_dir {args.data_dir} {"--tiny" if args.tiny else ""}'
    )
    print("Processing...")
    os.system(
        f'python3 process.py --input_dir {args.data_dir} --output_dir {args.output_dir} {"--tiny" if args.tiny else ""}'
    )
    print("Merging...")
    os.system(f"python3 merge.py --input_dir {args.output_dir}")
    print("Cleaning up...")
    os.system(f"python3 cleanup.py --input_dir {args.output_dir} --remove")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--data_dir", type=str, default="data", help="Directory to store data"
    )
    parser.add_argument(
        "--output_dir",
        type=str,
        default="output",
        help="Directory to store final output",
    )
    parser.add_argument(
        "--tiny", action="store_true", help="Use tiny dataset", default=False
    )
    args = parser.parse_args()
    main(args)

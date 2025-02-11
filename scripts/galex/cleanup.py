import argparse
import glob
import os
from tqdm.auto import tqdm

def main(args):
    files = glob.glob(args.input_dir + "*/*.hdf5")
    to_clean = [f for f in files if os.path.basename(f) != "001-of-001.hdf5"]
    to_not_clean = list(set(files) - set(to_clean))
    print(f"Found {len(files)} files, cleaning {len(to_clean)} files, preserving {len(to_not_clean)} files")
    if args.remove:
        for f in tqdm(to_clean):
            os.remove(f)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Cleanup files")
    parser.add_argument("--input_dir", type=str, help="Input directory", required=True)
    parser.add_argument("--remove", action="store_true", default=False, help="Remove files. If not passed, this is a dry run.")
    args = parser.parse_args()
    main(args)

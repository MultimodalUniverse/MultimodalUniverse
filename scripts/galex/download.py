import argparse
import os

def main(args):
    with open(args.filelist, "r") as filelist:
        urls = [u.strip() for u in filelist.readlines()]
    if args.tiny:
        urls = urls[:1]

    os.makedirs(args.output_dir, exist_ok=True)
    os.system(f"aria2c -j16 -s16 -x16 -c -d {args.output_dir} -Z {' '.join(urls)}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--filelist", type=str, default='filelist.txt', help="File containing URLs to download")
    parser.add_argument("--output_dir", type=str, default='.', help="Directory to save downloaded files")
    parser.add_argument("--tiny", action="store_true", default=False, help="Download small subset files")
    args = parser.parse_args()
    main(args)

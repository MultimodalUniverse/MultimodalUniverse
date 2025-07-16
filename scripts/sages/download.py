import argparse
import os

base_url = "https://casdc.china-vo.org/archive/SAGES-DR1/"
files = ["dr1-uv.fits"]#, "dr1s-gri.fits"]


def download(args):
    os.makedirs(args.output_dir, exist_ok=True)
    os.system(
        f'aria2c -x 16 -j 16 -s 16 -c -d {args.output_dir} -Z {" ".join([os.path.join(base_url, f) for f in files])}'
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download SAGES DR1 data")
    parser.add_argument("--output_dir", type=str, default=".", help="Output directory")
    args = parser.parse_args()
    download(args)

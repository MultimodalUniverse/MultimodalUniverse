import os
import argparse
import datasets
from astropile.utils import cross_match_datasets


def cross_match(
    left_path: str, 
    right_path: str, 
    cache_dir: str, 
    local_astropile_root: str = None,
    matching_radius: float = 1.0, # in arcseconds
    num_proc: int = 1,
):
    # Get paths
    if local_astropile_root is not None:
        left_path = os.path.join(local_astropile_root, left_path)
        right_path = os.path.join(local_astropile_root, right_path)

    # Load datasets
    left = datasets.load_dataset_builder(left_path, trust_remote_code=True)
    right = datasets.load_dataset_builder(right_path, trust_remote_code=True)

    print(f'Cross-matching datasets with matching radius {matching_radius} arcseconds...')

    # Cross-match datasets
    dset = cross_match_datasets(
        left,
        right,
        matching_radius=matching_radius,
        num_proc=num_proc,
    )

    dset.save_to_disk(cache_dir)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Cross-match two datasets')
    parser.add_argument('left', type=str, help='Path to the left dataset')
    parser.add_argument('right', type=str, help='Path to the right dataset')
    parser.add_argument('cache_dir', type=str, help='Path to the cache directory')
    parser.add_argument('--local_astropile_root', type=str, default=None, help='Path to the local astropile root')
    parser.add_argument('--matching_radius', type=float, default=1.0, help='Matching radius in arcseconds')
    parser.add_argument('--num_proc', type=int, default=31, help='Number of processes to use')

    args = parser.parse_args()

    cross_match(args.left, args.right, args.cache_dir, args.local_astropile_root, args.matching_radius, args.num_proc)
    
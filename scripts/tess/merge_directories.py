from pathlib import Path
import shutil
from tqdm import tqdm
import argparse

def merge_directories(directories, output_dir):
    output_dir = Path(output_dir)

    # Create the output directory if it doesn't exist
    output_dir.mkdir(parents=True, exist_ok=True)

    # Collect all top-level subdirectories from all input directories
    all_subdirs = {}
    for dir_path in directories:
        dir_path = Path(dir_path)
        for subdir in dir_path.iterdir():
            if subdir.is_dir():
                if subdir.name not in all_subdirs:
                    all_subdirs[subdir.name] = []
                all_subdirs[subdir.name].append(subdir)
    print(f"num collisions: {len([x for x in all_subdirs.keys() if len(all_subdirs[x]) > 1])} / {len(all_subdirs.keys())}")

    # Merge top-level subdirectories with the same name
    for subdir_name, subdir_paths in tqdm(all_subdirs.items()):
        output_subdir = output_dir / subdir_name
        output_subdir.mkdir(parents=True, exist_ok=True)

        # Renaming logic based on the number of subdirectories with the same name
        total_subdirs = len(subdir_paths)
        subdir_index = 1

        for subdir in subdir_paths:
            for item in subdir.iterdir():
                if item.is_file() and item.name.endswith(".hdf5"):
                    # Modify the filename from '001-of-001.hdf5' to '00x-of-00n.hdf5'
                    parts = item.name.split("-of-")
                    if len(parts) == 2 and parts[1].startswith("001"):
                        new_filename = f"{subdir_index:03}-of-{total_subdirs:03}.hdf5"
                    else:
                        new_filename = item.name

                    dest_path = output_subdir / new_filename
                    if not dest_path.exists():
                        shutil.copy2(item, dest_path)

            subdir_index += 1

    total_output_files = 0
    for subdir in output_dir.iterdir():
        total_output_files += len(list(subdir.iterdir()))
    num_input_files = {k:len(v) for k,v in all_subdirs.items()}
    total_input_files = sum(num_input_files.values())
    print(f"total output: {total_output_files}, total input: {total_input_files}")
    assert total_output_files == total_input_files

    print("Top-level subdirectories merged and renamed successfully.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_dirs", type=str, nargs="+", required=True)
    parser.add_argument("--output_dir", type=str, required=True)
    args = parser.parse_args()
    merge_directories(args.input_dirs, args.output_dir)

import argparse
import os
from pathlib import Path
from typing import List
from datasets import load_dataset_builder
import sys

INFO_KEYS = ['citation', 'description', 'homepage', 'license', 'version', 'acknowledgements']

def get_all_datasets() -> List[str]:
    # Collect all unique dataset names from the scripts directory
    all_datasets = set()
    # only consider directories in the scripts directory
    for file in os.listdir('scripts'):
        if os.path.isdir(os.path.join('scripts', file)):
            all_datasets.add(file)
    return sorted(list(all_datasets))

def get_info(dataset: str, info_keys: List[str] = ['citation']) -> dict:
    try:
        # Construct path to the dataset script
        script_path = Path('scripts') / f"{dataset}/{dataset}.py"
        if not script_path.exists():
            raise FileNotFoundError(f"Dataset script not found: {script_path}")
            
        # Load the dataset builder from the local script
        sys.path.insert(0, str(script_path.parent))
        builder = load_dataset_builder(str(script_path), trust_remote_code=True)
        sys.path.pop(0)
        
        info = builder.info
        
        # Get the requested information
        dataset_info = {}
        if "acknowledgements" in info_keys:
            if info_keys == INFO_KEYS:
                info_keys.remove("acknowledgements")
            else:
                # Remove all % from line starts 
                if info.citation.startswith("% ACKNOWLEDGEMENTS"):
                    dataset_info["acknowledgements"] = "\n".join([line.lstrip("% ") for line in info.citation.split("% CITATION\n")[0].split("\n")])
                else:
                    dataset_info["acknowledgements"] = "% ACKNOWLEDGEMENTS\nNot available"
        for key in info_keys:
            value = getattr(info, key, "Not available")
            dataset_info[key] = value if value else "Not available"
        return dataset_info
    except Exception as e:
        print(f"Error loading dataset {dataset}: {str(e)}", file=sys.stderr)
        return {key: "Error: Unable to load dataset information" for key in info_keys}

def format_info(datasets: List[str], info_keys: List[str] = ['citation']) -> List[str]:
    formatted_info = []
    for dataset in datasets:
        info = get_info(dataset, info_keys)
        if info:
            formatted_info.append(f"%%% {dataset}")
            for key, value in info.items():
                if key in ["citation", "acknowledgements"]:
                    formatted_info.append(f"\n{value}")
                else:
                    formatted_info.append(f"% {key.upper():15}\n{value}\n")
    return formatted_info

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Retrieve information for Hugging Face datasets. If neither --cite nor --acknowledge are specified, all information is retrieved.")
    parser.add_argument('-d', '--data', nargs='+', help='Specific datasets to retrieve information for.')
    parser.add_argument('-c', '--cite', action='store_true', help='Retrieve citations.')
    parser.add_argument('-a', '--acknowledge', action='store_true', help='Retrieve acknowledgements.')
    parser.add_argument('-o', '--output', default=None, help="Output file path. If not specified, print to stdout.")
    args = parser.parse_args()

    # Determine which information keys to retrieve
    info_keys = []
    if args.acknowledge:
        info_keys.append('acknowledgements')
    if args.cite:
        info_keys.append('citation')
    if not info_keys:
        info_keys = INFO_KEYS

    # Determine which datasets to retrieve information for
    if args.data:
        datasets = args.data
    else:
        datasets = get_all_datasets()

    # Retrieve and format the information
    formatted_info = format_info(datasets, info_keys)
    if not args.output:
        print("\n".join(formatted_info))
    else:
        with open(args.output, 'w') as f:
            f.write("\n".join(formatted_info))

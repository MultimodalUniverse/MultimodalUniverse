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

def load_dataset_builder_safely(script_path: Path):
    """Safely load a dataset builder from a script path."""
    if not script_path.exists():
        raise FileNotFoundError(f"Dataset script not found: {script_path}")
        
    sys.path.insert(0, str(script_path.parent))
    try:
        builder = load_dataset_builder(str(script_path), trust_remote_code=True)
    finally:
        sys.path.pop(0)
    return builder

def extract_acknowledgements(citation: str) -> str:
    """Extract acknowledgements from citation text if present."""
    if citation.startswith(r"% % ACKNOWLEDGEMENTS") or citation.startswith(r"% ACKNOWLEDGEMENTS"):
        return "\n".join([line.lstrip("% ") for line in citation.split("% CITATION\n")[0].split("\n")])
    return "Not available"

def get_info(dataset: str, info_keys: List[str] = ['citation']) -> dict:
    """
    Get information for datasets handling errors, missing information, and acknowledgements.
    Args:
        dataset (str): The name of the dataset.
        info_keys (List[str]): The keys to retrieve from the dataset info.
    Returns:
        A dictionary with the requested information.
    """
    script_path = Path('scripts') / f"{dataset}/{dataset}.py"
    
    try:
        builder = load_dataset_builder_safely(script_path)
    except Exception as e:
        print(f"Error loading dataset {dataset}: {str(e)}", file=sys.stderr)
        return {key: "Error: Unable to load dataset information" for key in info_keys}

    info = builder.info
    dataset_info = {}

    # Handle acknowledgements separately
    if "acknowledgements" in info_keys:
        if info_keys == INFO_KEYS:
            info_keys.remove("acknowledgements")
        else:
            dataset_info["acknowledgements"] = extract_acknowledgements(info.citation)

    # Get remaining information
    for key in info_keys:
        value = getattr(info, key, "Not available")
        dataset_info[key] = value if value else "Not available"

    return dataset_info

def format_info(datasets: List[str], info_keys: List[str] = ['citation'], check_missing: bool = False) -> List[str]:
    """
    Format information for datasets returning either a list of formatted information or a list of missing information.
    Args:
        datasets (List[str]): The datasets to retrieve information for.
        info_keys (List[str]): The keys to retrieve from the dataset info.
        check_missing (bool): Whether to return a list of missing information.
    Returns:
        A list of formatted information or a list of missing information.
    """
    formatted_info = []
    missing_info = {key: [] for key in info_keys}
    
    for dataset in datasets:
        info = get_info(dataset, info_keys)
        if info:
            # Track missing information
            for key in info_keys:
                if info[key] in ["Not available", "Error: Unable to load dataset information"]:
                    missing_info[key].append(dataset)
            
            # Format output as before
            formatted_info.append(f"%%% {dataset}")
            for key, value in info.items():
                if key in ["citation", "acknowledgements"]:
                    formatted_info.append(f"\n{value}")
                else:
                    formatted_info.append(f"% {key.upper():15}\n{value}\n")
    
    return missing_info if check_missing else formatted_info

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Retrieve information for Hugging Face datasets. If neither --cite nor --acknowledge are specified, all information is retrieved.")
    parser.add_argument('-d', '--data', nargs='+', help='Specific datasets to retrieve information for.')
    parser.add_argument('-c', '--cite', action='store_true', help='Retrieve citations.')
    parser.add_argument('-a', '--acknowledge', action='store_true', help='Retrieve acknowledgements.')
    parser.add_argument('-o', '--output', default=None, help="Output file path. If not specified, print to stdout.")
    parser.add_argument('-m', '--missing', action='store_true', help='Return list of datasets with missing information.')
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
    result = format_info(datasets, info_keys, args.missing)
    
    if args.missing:
        output = []
        for key, missing_datasets in result.items():
            if missing_datasets:
                output.append(f"Datasets missing {key}:")
                output.extend(f"  - {dataset}" for dataset in missing_datasets)
                output.append("")  # Empty line between sections
        output_text = "\n".join(output)
    else:
        output_text = "\n".join(result)

    if not args.output:
        print(output_text)
    else:
        with open(args.output, 'w') as f:
            f.write(output_text)

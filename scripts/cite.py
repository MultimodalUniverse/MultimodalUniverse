from bibtex_citations import *
import argparse 

#### Example use: 
#### python cite.py apogee gaia sdss

def print_citation_info(selected_datasets: list) -> str:
    for dataset in selected_datasets:
        if dataset in citation_instructions:
            print(f"%%% Citation instructions for {dataset} dataset:\n{citation_instructions[dataset]}")
        else:
            print(f"{dataset}: Dataset name not found.")

        if dataset in bibtex_entries:
            print(f"%%% BibTeX citations for {dataset} dataset:\n{bibtex_entries[dataset]}")
        else:
            print(f"{dataset}: Dataset name not found.")

            
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Print BibTeX citations for selected datasets.")
    parser.add_argument('datasets', metavar='datasets', type=str, nargs='+', help='List of selected datasets (e.g. "apogee sdss gaia") ')
    
    args = parser.parse_args()
    
    selected_datasets = args.datasets
    print_citation_info(selected_datasets)
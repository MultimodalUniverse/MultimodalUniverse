# Contributing to the MultiModalUniverse

First off, thank you for your interest in contributing to the MultiModalUniverse dataset! We welcome contributions from the community to help make this dataset more comprehensive and valuable for astronomical research, and once your contribution is merged with the main branch you will be officially recognized as a contributor to the project!

We recommend reading the [design documentation](https://github.com/MultiModalUniverse/MultiModalUniverse/blob/main/DESIGN.md) to get a feel for the aims and code design of the MultiModalUniverse.

And please take a moment to read this doc **before submitting a pull request**.

# Where do I go from here?

If you have a question, roadmap suggestion, or an idea for the MultiModalUniverse please create a [GitHub discussion](https://github.com/MultiModalUniverse/MultiModalUniverse/discussions). If you've found a bug, please [submit an issue](https://github.com/MultiModalUniverse/MultiModalUniverse/issues). If you want to add a dataset, you can read about preset data formats in the [design documentation](https://github.com/MultiModalUniverse/MultiModalUniverse/blob/main/DESIGN.md) - the process remains the same as described in this document.

## If you can implement your feature

If you can implement your proposed feature then [fork the MultiModalUniverse](https://docs.github.com/en/get-started/quickstart/fork-a-repo) and create a branch with a descriptive name.

Once you have your feature implemented, [open up a pull request](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request) and one of the MultiModal Universe admins will review the code and merge to main or come back with comments. If your pull request is connected to an issue or roadmap item please do not forget to link it.

## How to test your new dataset (HuggingFace)

Let's pretend you're trying to add data from a new source `my_data_source` (e.g. a survey, simulation set, etc). First, make a directory `MultiModalUniverse/scripts/my_data_source`, and populate with at least `build_parent_sample.py` and `my_data_source.py`.
- `build_parent_sample.py` should download the data and save it in the standard HDF5 file format.
- `my_data_source.py` is a HuggingFace dataset loading script for this data.
  
To test, there are two options:

1. Run `build_parent_sample.py` with `output_dir` pointing to `MultiModalUniverse/scripts/my_data_source`, which will download the data into the MultiModalUniverse scripts location. Then, when opening the PR you'll have to add a `.gitignore` file that indicates that the data files should be ignored so they don't get pushed to remote.
2. Run `build_parent_sample.py` with `output_dir` pointing elsewhere (e.g. to a scratch directory) and symlink `my_data_source.py` there. This is because the dataset loading script should be in the same directory as the HDF5 data (note that the dataset loading script must be named the same as the directory name)!

Then, run `load_dataset('/path/to/output_dir')` to ensure the dataset loading works properly.

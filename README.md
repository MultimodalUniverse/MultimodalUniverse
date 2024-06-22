# Multimodal Universe: Enabling Large-Scale Machine Learning with 70TBs of Astronomical Scientific Data
[![Dataset on HF](https://huggingface.co/datasets/huggingface/badges/resolve/main/dataset-on-hf-sm.svg)](https://huggingface.co/MultimodalUniverse) [![Demo on Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/MultimodalUniverse/MultimodalUniverse/blob/main/notebooks/getting_started.ipynb) [![Test](https://github.com/AstroPile/AstroPile_prototype/actions/workflows/tiny_dset_test.yml/badge.svg)](https://github.com/AstroPile/AstroPile_prototype/actions/workflows/tiny_dset_test.yml) [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)<!-- ALL-CONTRIBUTORS-BADGE:START - Do not remove or modify this section -->
[![All Contributors](https://img.shields.io/badge/all_contributors-28-orange.svg)](#contributors-)
<!-- ALL-CONTRIBUTORS-BADGE:END -->
## Overview

The Multimodal Universe dataset is a large scale collection of multimodal astronomical data, including images, spectra, and light curves, which aims to enable research into foundation models for astrophysics and beyond.

![image](assets/astropile.png)

## Installation and Usage
For a lightweight prototype of the functionality included in this repository, please see the [Lightweight Prototype](https://colab.research.google.com/drive/1t9dXqqeozrGjsx02q14a4Kmmp6GEhBYq?usp=sharing#scrollTo=yMKtJVxWlx24). In addition, previews (~1k examples) of all our datasets can be found on our [HuggingFace page](https://huggingface.co/MultimodalUniverse) and accessed via `load_dataset('MultimodalUniverse/dataset_name')`!

To use the full version of a dataset, first download the dataset using the `build_parent_sample.py` script for that dataset. For example, for the PLAsTiCC dataset, run
```bash
python scripts/plasticc/build_parent_sample.py /path/to/download/raw/data /path/to/save/dataset
```
Now the dataset can be loaded using the [HuggingFace Datasets](https://huggingface.co/docs/datasets/en/index) interface:
```py
from datasets import load_dataset

dset = load_dataset('/path/to/save/dataset/plasticc.py', trust_remote_code=True, split='train').with_format('numpy')
print(f'loaded dataset with {len(dset)} examples')
print(next(iter(dset)))
```

For further details on analyzing different types of modalities and training/evaluating benchmark models, please check out our [demo notebooks](https://github.com/MultimodalUniverse/MultimodalUniverse/tree/main/notebooks)!

## Datasets
The Multimodal Universe currently contains data from the following surveys/modalities:
| **Survey**           | **Modality**        | **Science Use Case** | **# samples** |
|----------------------|---------------------|----------------------|---------------|
| Legacy Surveys DR10  | Images              | Galaxies             | 124M          |
| Legacy Surveys North | Images              | Galaxies             | 15M           |
| HSC                  | Images              | Galaxies             | 477k          |
| BTS                  | Images              | Supernovae           | 400k          |
| JWST                 | Images              | Galaxies             | 300k          |
| Gaia BP/RP           | Spectra             | Stars                | 220M          |
| SDSS-II              | Spectra             | Galaxies, Stars      | 4M            |
| DESI                 | Spectra             | Galaxies             | 1M            |
| APOGEE SDSS-III      | Spectra             | Stars                | 716k          |
| GALAH                | Spectra             | Stars                | 325k          |
| Chandra              | Spectra             | Galaxies, Stars      | 129k          |
| VIPERS               | Spectra             | Galaxies             | 91k           |
| MaNGA SDSS-IV        | Hyperspectral Image | Galaxies             | 12k           |
| PLAsTiCC             | Time Series         | Time-varying objects | 3.5M          |
| TESS                 | Time Series         | Exoplanets           | 160k          |
| CfA Sample           | Time Series         | Supernovae           | 1k            |
| YSE                  | Time Series         | Supernovae           | 2k            |
| PS1 SNe Ia           | Time Series         | Supernovae           | 369           |
| DES Y3 SNe Ia        | Time Series         | Supernovae           | 248           |
| SNLS                 | Time Series         | Supernovae           | 239           |
| Foundation           | Time Series         | Supernovae           | 180           |
| CSP SNe Ia           | Time Series         | Supernovae           | 134           |
| Swift SNe Ia         | Time Series         | Supernovae           | 117           |
| Gaia                 | Tabular             | Stars                | 220M          |
| PROVABGS             | Tabular             | Galaxies             | 221k          |
| Galaxy10 DECaLS      | Tabular             | Galaxies             | 15k           |

## Architecture

![Processing](assets/astropile_data_processing.svg)

Illustration of the methodology behind the Multimodal Universe. Domain scientists with expertise in a given astronomical survey provide data download and formatting scripts through Pull Requests. All datasets are then downloaded from their original source and made available as Hugging Face datasets sharing a common data schema for each modality and associated metadata. End-users can then generate any combination of subsets using provided cross-matching utilities to generate multimodal datasets.

Please see the [Design Document](https://github.com/AstroPile/AstroPile_prototype/blob/main/DESIGN.md) for context about the project. 

## Contributors

#### Full Contribution List
<!-- ALL-CONTRIBUTORS-LIST:START - Do not remove or modify this section -->
<!-- prettier-ignore-start -->
<!-- markdownlint-disable -->
<table>
  <tbody>
    <tr>
      <td align="center" valign="top" width="14.28%"><a href="http://flanusse.net/"><img src="https://avatars.githubusercontent.com/u/861591?v=4?s=100" width="100px;" alt="Francois Lanusse"/><br /><sub><b>Francois Lanusse</b></sub></a><br /><a href="#projectManagement-EiffL" title="Project Management">📆</a> <a href="#example-EiffL" title="Examples">💡</a> <a href="https://github.com/MulitmodalUniverse/MultimodalUniverse/commits?author=EiffL" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/lhparker1"><img src="https://avatars.githubusercontent.com/u/86175266?v=4?s=100" width="100px;" alt="Liam Parker"/><br /><sub><b>Liam Parker</b></sub></a><br /><a href="#projectManagement-lhparker1" title="Project Management">📆</a> <a href="#example-lhparker1" title="Examples">💡</a> <a href="https://github.com/MulitmodalUniverse/MultimodalUniverse/commits?author=lhparker1" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://mb010.github.io/"><img src="https://avatars.githubusercontent.com/u/51884241?v=4?s=100" width="100px;" alt="Micah Bowles"/><br /><sub><b>Micah Bowles</b></sub></a><br /><a href="#projectManagement-mb010" title="Project Management">📆</a> <a href="#example-mb010" title="Examples">💡</a> <a href="https://github.com/MulitmodalUniverse/MultimodalUniverse/commits?author=mb010" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/mhuertascompany"><img src="https://avatars.githubusercontent.com/u/22987973?v=4?s=100" width="100px;" alt="mhuertascompany"/><br /><sub><b>mhuertascompany</b></sub></a><br /><a href="#projectManagement-mhuertascompany" title="Project Management">📆</a> <a href="#example-mhuertascompany" title="Examples">💡</a> <a href="https://github.com/MulitmodalUniverse/MultimodalUniverse/commits?author=mhuertascompany" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://mjjsmith.com/"><img src="https://avatars.githubusercontent.com/u/8194280?v=4?s=100" width="100px;" alt="Mike Smith"/><br /><sub><b>Mike Smith</b></sub></a><br /><a href="#projectManagement-Smith42" title="Project Management">📆</a> <a href="#example-Smith42" title="Examples">💡</a> <a href="https://github.com/MulitmodalUniverse/MultimodalUniverse/commits?author=Smith42" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/helenqu"><img src="https://avatars.githubusercontent.com/u/8826297?v=4?s=100" width="100px;" alt="Helen Qu"/><br /><sub><b>Helen Qu</b></sub></a><br /><a href="#projectManagement-helenqu" title="Project Management">📆</a> <a href="#example-helenqu" title="Examples">💡</a> <a href="https://github.com/MulitmodalUniverse/MultimodalUniverse/commits?author=helenqu" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/ado8"><img src="https://avatars.githubusercontent.com/u/32152325?v=4?s=100" width="100px;" alt="Aaron"/><br /><sub><b>Aaron</b></sub></a><br /><a href="#example-ado8" title="Examples">💡</a> <a href="https://github.com/MulitmodalUniverse/MultimodalUniverse/commits?author=ado8" title="Code">💻</a></td>
    </tr>
    <tr>
      <td align="center" valign="top" width="14.28%"><a href="https://cdtdis.bigdata.cam.ac.uk/author/boyd01/"><img src="https://avatars.githubusercontent.com/u/97113179?v=4?s=100" width="100px;" alt="Ben Boyd"/><br /><sub><b>Ben Boyd</b></sub></a><br /><a href="#example-benboyd97" title="Examples">💡</a> <a href="https://github.com/MulitmodalUniverse/MultimodalUniverse/commits?author=benboyd97" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/havok2063"><img src="https://avatars.githubusercontent.com/u/1836302?v=4?s=100" width="100px;" alt="Brian Cherinka"/><br /><sub><b>Brian Cherinka</b></sub></a><br /><a href="https://github.com/MulitmodalUniverse/MultimodalUniverse/commits?author=havok2063" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://connorjstone.com/"><img src="https://avatars.githubusercontent.com/u/78555321?v=4?s=100" width="100px;" alt="Connor Stone, PhD"/><br /><sub><b>Connor Stone, PhD</b></sub></a><br /><a href="#example-ConnorStoneAstro" title="Examples">💡</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/David-Chemaly"><img src="https://avatars.githubusercontent.com/u/66142655?v=4?s=100" width="100px;" alt="David Chemaly"/><br /><sub><b>David Chemaly</b></sub></a><br /><a href="#example-David-Chemaly" title="Examples">💡</a> <a href="https://github.com/MulitmodalUniverse/MultimodalUniverse/commits?author=David-Chemaly" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://www.ast.cam.ac.uk/people/Erin.Hayes"><img src="https://avatars.githubusercontent.com/u/53277330?v=4?s=100" width="100px;" alt="Erin Hayes"/><br /><sub><b>Erin Hayes</b></sub></a><br /><a href="#example-erinhay" title="Examples">💡</a> <a href="https://github.com/MulitmodalUniverse/MultimodalUniverse/commits?author=erinhay" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://henrysky.github.io/"><img src="https://avatars.githubusercontent.com/u/28623434?v=4?s=100" width="100px;" alt="Henry Leung"/><br /><sub><b>Henry Leung</b></sub></a><br /><a href="https://github.com/MulitmodalUniverse/MultimodalUniverse/commits?author=henrysky" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://www.jociuca.com/"><img src="https://avatars.githubusercontent.com/u/13675150?v=4?s=100" width="100px;" alt="Ioana Ciucă"/><br /><sub><b>Ioana Ciucă</b></sub></a><br /><a href="#content-errai34" title="Content">🖋</a></td>
    </tr>
    <tr>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/al-jshen"><img src="https://avatars.githubusercontent.com/u/22137276?v=4?s=100" width="100px;" alt="Jeff Shen"/><br /><sub><b>Jeff Shen</b></sub></a><br /><a href="https://github.com/MulitmodalUniverse/MultimodalUniverse/commits?author=al-jshen" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/jeraud"><img src="https://avatars.githubusercontent.com/u/41162303?v=4?s=100" width="100px;" alt="jeraud"/><br /><sub><b>jeraud</b></sub></a><br /><a href="#example-jeraud" title="Examples">💡</a> <a href="https://github.com/MulitmodalUniverse/MultimodalUniverse/commits?author=jeraud" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://jwuphysics.github.io/"><img src="https://avatars.githubusercontent.com/u/3582121?v=4?s=100" width="100px;" alt="John F. Wu"/><br /><sub><b>John F. Wu</b></sub></a><br /><a href="#content-jwuphysics" title="Content">🖋</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/CambridgeAstroStat"><img src="https://avatars.githubusercontent.com/u/35563929?v=4?s=100" width="100px;" alt="CambridgeAstroStat"/><br /><sub><b>CambridgeAstroStat</b></sub></a><br /><a href="#mentoring-CambridgeAstroStat" title="Mentoring">🧑‍🏫</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://kartheikiyer.github.io/"><img src="https://avatars.githubusercontent.com/u/14657226?v=4?s=100" width="100px;" alt="Kartheik Iyer"/><br /><sub><b>Kartheik Iyer</b></sub></a><br /><a href="https://github.com/MulitmodalUniverse/MultimodalUniverse/commits?author=kartheikiyer" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://ltmeyer.github.io/"><img src="https://avatars.githubusercontent.com/u/13559010?v=4?s=100" width="100px;" alt="Lucas Meyer"/><br /><sub><b>Lucas Meyer</b></sub></a><br /><a href="https://github.com/MulitmodalUniverse/MultimodalUniverse/commits?author=LTMeyer" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/mattgrayling"><img src="https://avatars.githubusercontent.com/u/12443427?v=4?s=100" width="100px;" alt="Matthew Grayling"/><br /><sub><b>Matthew Grayling</b></sub></a><br /><a href="#example-mattgrayling" title="Examples">💡</a> <a href="https://github.com/MulitmodalUniverse/MultimodalUniverse/commits?author=mattgrayling" title="Code">💻</a></td>
    </tr>
    <tr>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/maja-jablonska"><img src="https://avatars.githubusercontent.com/u/23705704?v=4?s=100" width="100px;" alt="Maja Jabłońska"/><br /><sub><b>Maja Jabłońska</b></sub></a><br /><a href="https://github.com/MulitmodalUniverse/MultimodalUniverse/commits?author=maja-jablonska" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/mwalmsley"><img src="https://avatars.githubusercontent.com/u/7740526?v=4?s=100" width="100px;" alt="Mike Walmsley"/><br /><sub><b>Mike Walmsley</b></sub></a><br /><a href="#example-mwalmsley" title="Examples">💡</a> <a href="https://github.com/MulitmodalUniverse/MultimodalUniverse/commits?author=mwalmsley" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/MilesCranmer"><img src="https://avatars.githubusercontent.com/u/7593028?v=4?s=100" width="100px;" alt="Miles Cranmer"/><br /><sub><b>Miles Cranmer</b></sub></a><br /><a href="#content-MilesCranmer" title="Content">🖋</a></td>
      <td align="center" valign="top" width="14.28%"><a href="http://pmelchior.net/"><img src="https://avatars.githubusercontent.com/u/1463403?v=4?s=100" width="100px;" alt="Peter Melchior"/><br /><sub><b>Peter Melchior</b></sub></a><br /><a href="https://github.com/MulitmodalUniverse/MultimodalUniverse/commits?author=pmelchior" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/juramaga"><img src="https://avatars.githubusercontent.com/u/5503235?v=4?s=100" width="100px;" alt="Rafael Martínez-Galarza"/><br /><sub><b>Rafael Martínez-Galarza</b></sub></a><br /><a href="https://github.com/MulitmodalUniverse/MultimodalUniverse/commits?author=juramaga" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/tom-hehir"><img src="https://avatars.githubusercontent.com/u/148493038?v=4?s=100" width="100px;" alt="Tom Hehir"/><br /><sub><b>Tom Hehir</b></sub></a><br /><a href="#example-tom-hehir" title="Examples">💡</a> <a href="https://github.com/MulitmodalUniverse/MultimodalUniverse/commits?author=tom-hehir" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/shirleysurelyho"><img src="https://avatars.githubusercontent.com/u/3279839?v=4?s=100" width="100px;" alt="Shirley Ho"/><br /><sub><b>Shirley Ho</b></sub></a><br /><a href="#fundingFinding-shirleysurelyho" title="Funding Finding">🔍</a> <a href="#content-shirleysurelyho" title="Content">🖋</a></td>
    </tr>
  </tbody>
</table>

<!-- markdownlint-restore -->
<!-- prettier-ignore-end -->

<!-- ALL-CONTRIBUTORS-LIST:END -->

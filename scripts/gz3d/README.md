# GZ3D Dataset

The [GZ3D](https://www.zooniverse.org/projects/klmasters/galaxy-zoo-3d) (Galaxy Zoo 3D) collected annotations of stars, bars, spiral arms, and the centers of 29,813 MaNGA galaxies. The respective PNG of the target is included in the dataset. It is published as a Value Added Catalog (VAC) as part of SDSS data release 17. Each segmentation is an aggregate of 15 human annotations. The associated catalog contains summaries of the respective classifications as collected by the Galaxy Zoo collaboration in relevant classification project. The original code on which [utils.py](./utils.py) is tracked [here]("https://github.com/CKrawczyk/GZ3D_production/").

## Data Download

All data are downloaded from the [SDSS Science Archive Server (SAS)](https://data.sdss.org/sas/dr17/env/MANGA_MORPHOLOGY/galaxyzoo3d/v4_0_0/)

[This script](./build_parent_sample.py) can be used to download GZ3D. To download a sample of 5, use the `--tiny` flag:
```bash
python build_parent_sample.py --tiny
```
This will download all of the FITS files, group them into folders for each healpix value they belong to. In each folder a single sample is then represented as a single hdf5 file. The full data is approximately 8GB in size. The direct download is likely rate limited by the host.

> NOTE: The downloaded fits files are deleted if the `--keep_fits` flag is not set.

<!-- We suggest downloading the data using globus from a registered host. -->

<!-- ### sdss-access

Alternatively, you can use the `access_tranfer` Python script to download the data with the [sdss-access](https://sdss-access.readthedocs.io/en/latest/) Python package, which uses parallelized `rsync` streams to download the data. This tool organizes the output download directory structure to mirror the official SDSS SAS.

To download all summary files, cubes and maps, run:
```bash
python access_transfer.py --destination_path .
``` -->

## Data Selection

Currently no selections are made when generating the dataset. We note that spiral and bar segmentation maps may contain annotations, even if that galaxy has a low fraction of votes from the respective Galaxy Zoo campaign, effectively stating there is no visible bar or spiral arms.

## Dataset Structure

See the `demo_gz3d.ipynb` [Jupyter notebook](./demo_gz3d.ipynb) for an example of how to load, plot and interact with the dataset. This notebook downloads 10 samples of the dataset, and iterates over the objects plotting the false color image of the galaxy shown to the labellers, and the segmentation maps containing the vote count on a pixel wise basis.


```python
from datasets import load_dataset

gz3d = load_dataset('gz3d.py', trust_remote_code=True, split='train', streaming=True)
gz3d = iter(gz3d.with_format('numpy'))

ii = next(gz3d)
```

### Feature Datamodel

The dataset structure is organized with the following features:

Metadata:
- `total_classifications`: The total number of Galaxy Zoo classifications made for this target. Note this is not the number of segementations but the number of citizen scientists who voted on if the galaxy is a spiral or barred galaxy.
- `healpix`: The healpix value in which the subject lies.
- `ra`: Right ascension of the target in degrees.
- `dec`: Declination of the target in degrees.
- `object_id`: The MaNGA ID (unique to each galaxy, not unique to each plate IFU).

Image data:
- `image`: The RGB false color image of the target (MaNGA) galaxy. Data format: `[H,W,C] = [512,512,3]` 16-bit integer.
- `scale`: The angular extent of a pixel in the image in degrees.
- `segmentation`: The segmentation map data. Each key refers to an ordered list of four elements each referring to a specific class.
    - `class`: The class of the segmentation map. One of `center, star, spiral, bar`
    - `vote_fraction`: The fraction of `total_classifications` that were positive towards this class for a class of spiral or bar (e.g. `20/50` total classifications state the subject is barred -> `0.4`). `-1.0` flags that the class is either center or star as there are no votes for these classes. `-0.5` flags if the total_classifications are 0 for the subject.
    - `array`: The segmentation map itself. Values range between 0 and 15, with the value at a given pixel indicating the number of users who included that region in their annotation for the respective class. I.e. a value of 5.0 indicates 5 of the 15 users marked segmentation masks which included that pixel. Data format: `[H,W,C] = [512,512]` 8-bit integer.


# Cite

If you use this dataset, please acknowledge the original publication of the GZ3D dataset and provide credit to the authors by citing the GZ3D dataset:

```
@article{masters2021galaxy,
    author = {Masters, Karen L and Krawczyk, Coleman and Shamsi, Shoaib and Todd, Alexander and Finnegan, Daniel and Bershady, Matthew and Bundy, Kevin and Cherinka, Brian and Fraser-McKelvie, Amelia and Krishnarao, Dhanesh and Kruk, Sandor and Lane, Richard R and Law, David and Lintott, Chris and Merrifield, Michael and Simmons, Brooke and Weijmans, Anne-Marie and Yan, Renbin},
    title = "{Galaxy Zoo: 3D - crowdsourced bar, spiral, and foreground star masks for MaNGA target galaxies}",
    journal = {Monthly Notices of the Royal Astronomical Society},
    volume = {507},
    number = {3},
    pages = {3923-3935},
    year = {2021},
    month = {08},
    doi = {10.1093/mnras/stab2282},
}
```


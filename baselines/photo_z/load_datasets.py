# Decide on image and spectrum datasets

import sys
from datasets import load_dataset, load_dataset_builder
sys.path.append('/mnt/home/lparker/Documents/AstroFoundationModel/AstroPile_prototype/')
from astropile.utils import cross_match_datasets

image_dataset = 'decals16'
spectrum_dataset = 'sdss16b'

# Load Dataset Builders 
print(f'Loading {image_dataset} dataset builder')
image_builder = load_dataset_builder(f'/mnt/ceph/users/polymathic/AstroPile/{image_dataset}/{image_dataset}.py', trust_remote_code=True)

print(f'Loading {spectrum_dataset} dataset builder')
spectrum_builder = load_dataset_builder(f'/mnt/ceph/users/polymathic/AstroPile/{spectrum_dataset}/{spectrum_dataset}.py', trust_remote_code=True)

# Cross-Match Datasets with AstroPile
cross_matched_dset = cross_match_datasets(image_builder, spectrum_builder,
                                      matching_radius=1.0,
                                      )

# You have to tell the cross-matched dataset which format you want to use
cross_matched_dset.set_format('torch')

# Save the cross-matched dataset
cross_matched_dset.save_to_disk(f'/mnt/ceph/users/polymathic/mmoma/datasets/{image_dataset}_{spectrum_dataset}')
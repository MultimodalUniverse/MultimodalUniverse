# Copyright 2020 The HuggingFace Datasets Authors and the current dataset script contributor.
#
# Licensed under the Apache License, Version 2.0 (the "License");
"""Dataset of SDSS DR16 Spectra"""

import json
import os
import glob
import h5py
import datasets
import numpy as np

_CITATION = """
"""

_DESCRIPTION = """\
This dataset is contains roughly 1.6 million SDSS spectra of galaxies, along with their redshifts,
redshift errors, target ids, noise on the spectra readings, RA, DEC.
"""

_HOMEPAGE = ""

_LICENSE = ""

_URLS = {
    "sdss_spectra": "https://users.flatironinstitute.org/~lparker/sdss_spectra.h5",
}

class SDSSSpectra(datasets.GeneratorBasedBuilder):
    VERSION = datasets.Version("1.0.0")
    BUILDER_CONFIGS = [
        datasets.BuilderConfig(name="sdss_spectra", version=VERSION, description="This covers all SDSS spectra in DR16"),
    ]

    DEFAULT_CONFIG_NAME = "sdss_spectra"  # It's not mandatory to have a default configuration. Just use one if it make sense.

    def _info(self):
        if self.config.name == "sdss_spectra":  # This is the name of the configuration selected in BUILDER_CONFIGS above
            features = datasets.Features(
                {
                    "spec": datasets.Array2D(shape=(3921,1), dtype='float32'),
                    "ivar": datasets.Array2D(shape=(3921,1), dtype='float32'),
                    "target_id": datasets.Value("int64"),
                    "z": datasets.Value("float32"),
                    "zerr": datasets.Value("float32"),
                    "obj_ra": datasets.Value("float32"),
                    "obj_dec": datasets.Value("float32"),
                }
            )
        else:  # This is an example to show how to have different features for "first_domain" and "second_domain"
            raise NotImplementedError("Only the sdss_spectra configuration is implemented for now")
        
        return datasets.DatasetInfo(
            description=_DESCRIPTION,
            features=features,  
            homepage=_HOMEPAGE,
            license=_LICENSE,
            citation=_CITATION,
        )

    def _split_generators(self, dl_manager):
        urls = _URLS[self.config.name]
        data_dir = dl_manager.download_and_extract(urls)
        return [
            datasets.SplitGenerator(
                name=datasets.Split.TRAIN,
                # These kwargs will be passed to _generate_examples
                gen_kwargs={
                    "filepath": data_dir,
                    "split": "train",
                },
            ),
            datasets.SplitGenerator(
                name=datasets.Split.TEST,
                # These kwargs will be passed to _generate_examples
                gen_kwargs={
                    "filepath": data_dir,
                    "split": "test"
                },
            ),
        ]

    # method parameters are unpacked from `gen_kwargs` as given in `_split_generators`
    def _generate_examples(self, filepath, split):
        """ Yields examples. """
        with h5py.File(filepath) as d:
            
            for i in range(8):

                z = d[str(i)]['z']
                zerr = d[str(i)]['zerr']
                ivar = d[str(i)]['ivar']
                spec = d[str(i)]['spec']
                obj_ra = d[str(i)]['obj_ra']
                obj_dec = d[str(i)]['obj_dec']
                target_id = d[str(i)]['target_id']

                dset_size = len(z)

                if split == 'train':
                    dset_range = (0, int(0.8*dset_size))
                else:
                    dset_range = (int(0.8*dset_size), dset_size)

                for j in range(dset_range[0], dset_range[1]):
                    yield str(i) + str(j), {
                        "spec": np.reshape(spec[j], [-1, 1]).astype('float32'),
                        "ivar": np.reshape(ivar[j], [-1, 1]).astype('float32'),
                        "z": z[j],
                        "zerr": zerr[j],
                        "target_id": target_id[j],
                        "obj_ra": obj_ra[j],
                        "obj_dec": obj_dec[j],
                    }
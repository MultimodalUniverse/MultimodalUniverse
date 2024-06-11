# HSC Dataset Collection 

We gather in this folder all the scripts and queries used to build the HSC parent sample.

## Sample selection

We build our sample by using only a cmodel imag cut of 22.5, plus some basic full depth full color selection and quality cuts. We use the following queries to build the parent sample:

- HSC PDR3 deep/ultra deep: sql query [pdr3_dud_22.5.sql](pdr3_dud_22.5.sql)

The same strategy could be applied to the wide survey, but that requires more storage space.

## Data Download and Cutout Extraction

To generate the parent cutout samples, we use the [unagi](https://github.com/dr-guangtou/unagi) package and the custom script [build_parent_sample.py](build_parent_sample.py).

The first step is to run the sql query to generate the parent catalog:
To generate the parent sample, run the following:
```bash
pip install git+https://github.com/eiffl/unagi.git@update

export SSP_PDR_USR=[your hsc username]
export SSP_PDR_PWD=[your hsc password]

# To build the deep sample
python build_parent_sample.py pdr3_dud_22.5.sql output_dir --rerun pdr3_dud_rev
```


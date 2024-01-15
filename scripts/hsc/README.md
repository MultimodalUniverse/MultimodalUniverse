# HSC Dataset Collection 

We gather in this folder all the scripts and queries used to build the HSC parent sample.

## Sample selection

We build our entire sample by using only a cmodel imag cut of 22.5, plus some basic full depth full color selection and quality cuts. We extract two catalogs, one from the wide and one from the deep/ultra deep survey.

- HSC PDR3 wide: sql query [pdr3_wide_22.5.sql](pdr3_wide_22.5.sql)
- HSC PDR3 deep/ultra deep: sql query [pdr3_dud_22.5.sql](pdr3_dud_22.5.sql)

## Cutout extraction

To generate the parent cutout samples, we use the [unagi](https://github.com/dr-guangtou/unagi) package and the custom script [build_parent_sample.py](build_parent_sample.py).

To generate the parent sample, run the following:
```bash
pip install git+https://github.com/eiffl/unagi.git@update

export SSP_PDR_USR=[your hsc username]
export SSP_PDR_PWD=[your hsc password]

# To build the deep sample
python build_parent_sample.py pdr3_dud_22.5.sql output_dir --rerun pdr3_dud_rev

# To build the wide sample
python build_parent_sample.py pdr3_wide_22.5.sql output_dir --rerun pdr3_wide
```


### Things to do/check:
- [ ] Should we store the WCS?
- [ ] How do we properly define the units of the images?
- [ ] In the sql call, make sure the column retain the name of the table (not the case at the moment)

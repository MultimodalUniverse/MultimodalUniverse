ASTROPILE_ROOT := /mnt/ceph/users/polymathic/AstroPile_v1
DATA_DIR := /mnt/ceph/users/polymathic/external_data/astro

.PHONY: all apogee desi sdss decals

all: apogee desi sdss decals

# Create the parent sample for APOGEE
$(ASTROPILE_ROOT)/apogee:
	cd apogee; \
	python build_parent_sample.py --apogee_data_path=$(DATA_DIR)/APOGEE \
	                              --output_dir=$(ASTROPILE_ROOT)/apogee \
	                              --num_processes 32; \
    cp apogee.py $(ASTROPILE_ROOT)/apogee/apogee.py; \
	cd ..
apogee: $(ASTROPILE_ROOT)/apogee

# Create the parent sample for desi
$(ASTROPILE_ROOT)/desi:
	cd desi; \
	python build_parent_sample.py $(DATA_DIR)/DESI \
	                              $(ASTROPILE_ROOT)/desi \
	                              --num_processes 32; \
	cp desi.py $(ASTROPILE_ROOT)/desi/desi.py; \
	cd ..
desi: $(ASTROPILE_ROOT)/desi

# Create the parent sample for SDSS
$(ASTROPILE_ROOT)/sdss:
	cd sdss; \
	python build_parent_sample.py $(DATA_DIR)/SDSS \
	                              $(ASTROPILE_ROOT)/sdss \
	                              --num_processes 32; \
	cp sdss.py $(ASTROPILE_ROOT)/sdss/sdss.py; \
	cd ..
sdss: $(ASTROPILE_ROOT)/sdss

# Create the parent sample for DECALS
$(ASTROPILE_ROOT)/decals:
	cd decals; \
	python build_parent_sample.py $(DATA_DIR)/DECALS_Stein_et_al \
	                              $(ASTROPILE_ROOT)/decals \
	                              --num_processes 32; \
	cp decals.py $(ASTROPILE_ROOT)/decals/decals.py; \
	cd ..
decals: $(ASTROPILE_ROOT)/decals
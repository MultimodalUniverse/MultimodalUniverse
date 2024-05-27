ASTROPILE_ROOT := /mnt/ceph/users/polymathic/AstroPile_v1
DATA_DIR := /mnt/ceph/users/polymathic/external_data/astro

.PHONY: all apogee desi sdss decals plasticc jwst gz10

all: apogee desi sdss decals plasticc jwst gz10

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

# Create the parent sample for plasticc
$(ASTROPILE_ROOT)/plasticc:
	cd plasticc; \
    python build_parent_sample.py $(DATA_DIR)/PLAsTiCC \
                                  $(ASTROPILE_ROOT)/plasticc \
                                  --num_processes 32; \
    cp plasticc.py $(ASTROPILE_ROOT)/plasticc/plasticc.py; \
    cd ..
plasticc: $(ASTROPILE_ROOT)/plasticc

# Create the parent sample for jwst
$(ASTROPILE_ROOT)/jwst:
	cd jwst; \
	mkdir -p $(DATA_DIR)/JWST; \
	python build_parent_sample.py ngdeep --image_dir=$(DATA_DIR)/JWST \
								  --output_dir=$(ASTROPILE_ROOT)/jwst; \
	python build_parent_sample.py primer-cosmos-east --image_dir=$(DATA_DIR)/JWST \
								  --output_dir=$(ASTROPILE_ROOT)/jwst; \
	python build_parent_sample.py primer-uds --image_dir=$(DATA_DIR)/JWST \
								  --output_dir=$(ASTROPILE_ROOT)/jwst; \
	python build_parent_sample.py gds --image_dir=$(DATA_DIR)/JWST \
								  --output_dir=$(ASTROPILE_ROOT)/jwst; \
	python build_parent_sample.py gdn --image_dir=$(DATA_DIR)/JWST \
								  --output_dir=$(ASTROPILE_ROOT)/jwst; \
	python build_parent_sample.py ceers-full --image_dir=$(DATA_DIR)/JWST \
								  --output_dir=$(ASTROPILE_ROOT)/jwst; \
    cp jwst.py $(ASTROPILE_ROOT)/jwst/jwst.py; \
	cd ..
jwst: $(ASTROPILE_ROOT)/jwst

# Create the parent sample for GZ10 classification data
$(ASTROPILE_ROOT)/gz10:
	cd gz10; \
	python build_parent_sample.py $(DATA_DIR)/Galaxy10_DECals.h5 \
	                              $(ASTROPILE_ROOT)/gz10; \
	cp gz10.py $(ASTROPILE_ROOT)/gz10/gz10.py; \
	cd ..
gz10: $(ASTROPILE_ROOT)/gz10
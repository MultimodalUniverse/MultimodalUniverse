ASTROPILE_ROOT := /mnt/ceph/users/polymathic/AstroPile_v1
DATA_DIR := /mnt/ceph/users/polymathic/external_data/astro

.PHONY: all apogee desi sdss decals plasticc jwst gz10 hsc vipers gaia desi_provabgs yse des_y3_sne_ia swift_sne_ia ps1_sne_ia snls foundation btsbot manga tess

all: apogee desi sdss decals plasticc jwst gz10 vipers hsc gaia desi_provabgs yse des_y3_sne_ia swift_sne_ia ps1_sne_ia snls foundation btsbot manga tess

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

# Create the parent sample for ssl_legacysurvey
$(ASTROPILE_ROOT)/ssl_legacysurvey:
	cd ssl_legacysurvey; \
	python build_parent_sample.py $(DATA_DIR)/DECALS_Stein_et_al \
	                              $(ASTROPILE_ROOT)/decals \
	                              --num_processes 32; \
	cp ssl_legacysurvey.py $(ASTROPILE_ROOT)/ssl_legacysurvey/ssl_legacysurvey.py; \
	cd ..
ssl_legacysurvey: $(ASTROPILE_ROOT)/ssl_legacysurvey

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
	for survey in ngdeep primer-cosmos-east primer-uds gds gdn ceers-full; do \
		python build_parent_sample.py $$survey --image_dir=$(DATA_DIR)/JWST \
			--output_dir=$(ASTROPILE_ROOT)/jwst; \
	done; \
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

# Create parent sample for vipers
$(ASTROPILE_ROOT)/vipers:
	cd vipers; \
	python build_parent_sample.py $(ASTROPILE_ROOT)/vipers \
                                  --num_processes 32; \
	cp vipers.py $(ASTROPILE_ROOT)/vipers/vipers.py; \
	cd ..
vipers: $(ASTROPILE_ROOT)/vipers

# Create parent sample for hsc
$(ASTROPILE_ROOT)/hsc:
	cd hsc; \
	python build_parent_sample.py pdr3_dud_22.5.sql $(ASTROPILE_ROOT)/hsc --rerun pdr3_dud --num_processes 1;\
	cp hsc.py $(ASTROPILE_ROOT)/hsc/hsc.py; \
	cd ..
hsc: $(ASTROPILE_ROOT)/hsc

# create parent sample for Gaia
$(ASTROPILE_ROOT)/gaia:
	cd gaia; \
	mkdir -p $(DATA_DIR)/Gaia; \
	mkdir -p $(ASTROPILE_ROOT)/gaia; \
	python3 download_parts.py --aria2 --output_dir $(DATA_DIR)/Gaia; \
	python3 merge_parts.py --input_dir $(DATA_DIR)/Gaia --output_file $(DATA_DIR)/Gaia/merged.hdf5; \
	python3 healpixify.py --input_file $(DATA_DIR)/Gaia/merged.hdf5 --output_dir $(ASTROPILE_ROOT)/gaia --nside 16 --num_procs 1; \
	cp gaia.py $(ASTROPILE_ROOT)/gaia/gaia.py; \
	cd ..
gaia: $(ASTROPILE_ROOT)/gaia

# create parent sample for desi_provabgs
$(ASTROPILE_ROOT)/desi_provabgs:
	cd desi_provabgs; \
	mkdir -p $(DATA_DIR)/DESI_PROVABGS; \
	mkdir -p $(ASTROPILE_ROOT)/desi_provabgs; \
	python3 build_parent_sample.py $(DATA_DIR)/DESI_PROVABGS $(ASTROPILE_ROOT)/desi_provabgs; \
	cp desi_provabgs.py $(ASTROPILE_ROOT)/desi_provabgs/desi_provabgs.py; \
	cd ..
desi_provabgs: $(ASTROPILE_ROOT)/desi_provabgs


# create parent sample for yse
$(ASTROPILE_ROOT)/yse:
	cd yse; \
	mkdir -p $(DATA_DIR)/yse; \
	mkdir -p $(ASTROPILE_ROOT)/yse; \
	python3 download_data.py --destination_path $(DATA_DIR)/yse; \
	python3 build_parent_sample.py --yse_data_path $(DATA_DIR)/yse --output_dir $(ASTROPILE_ROOT)/yse --dirty; \
	cp yse.py $(ASTROPILE_ROOT)/yse/yse.py; \
	cd ..
yse: $(ASTROPILE_ROOT)/yse


# create parent sample for des_y3_sne_ia
$(ASTROPILE_ROOT)/des_y3_sne_ia:
	cd des_y3_sne_ia; \
	mkdir -p $(DATA_DIR)/des_y3_sne_ia; \
	mkdir -p $(ASTROPILE_ROOT)/des_y3_sne_ia; \
	python3 download_data.py --destination_path $(DATA_DIR)/des_y3_sne_ia; \
	python3 build_parent_sample.py $(DATA_DIR)/des_y3_sne_ia $(ASTROPILE_ROOT)/des_y3_sne_ia --dirty; \
	cp des_y3_sne_ia.py $(ASTROPILE_ROOT)/des_y3_sne_ia/des_y3_sne_ia.py; \
	cd ..
des_y3_sne_ia: $(ASTROPILE_ROOT)/des_y3_sne_ia


# create parent sample for swift_sne_ia
$(ASTROPILE_ROOT)/swift_sne_ia:
	cd swift_sne_ia; \
	mkdir -p $(DATA_DIR)/swift_sne_ia; \
	mkdir -p $(ASTROPILE_ROOT)/swift_sne_ia; \
	python3 download_data.py --destination_path $(DATA_DIR)/swift_sne_ia; \
	python3 build_parent_sample.py $(DATA_DIR)/swift_sne_ia $(ASTROPILE_ROOT)/swift_sne_ia --dirty; \
	cp swift_sne_ia.py $(ASTROPILE_ROOT)/swift_sne_ia/swift_sne_ia.py; \
	cd ..
swift_sne_ia: $(ASTROPILE_ROOT)/swift_sne_ia

# create parent sample for ps1_sne_ia
$(ASTROPILE_ROOT)/ps1_sne_ia:
	cd ps1_sne_ia; \
	mkdir -p $(DATA_DIR)/ps1_sne_ia; \
	mkdir -p $(ASTROPILE_ROOT)/ps1_sne_ia; \
	python3 download_data.py --destination_path $(DATA_DIR)/ps1_sne_ia; \
	python3 build_parent_sample.py $(DATA_DIR)/ps1_sne_ia $(ASTROPILE_ROOT)/ps1_sne_ia --dirty; \
	cp ps1_sne_ia.py $(ASTROPILE_ROOT)/ps1_sne_ia/ps1_sne_ia.py; \
	cd ..
ps1_sne_ia: $(ASTROPILE_ROOT)/ps1_sne_ia

# create parent sample for snls
$(ASTROPILE_ROOT)/snls:
	cd snls; \
	mkdir -p $(DATA_DIR)/snls; \
	mkdir -p $(ASTROPILE_ROOT)/snls; \
	python3 download_data.py --destination_path $(DATA_DIR)/snls; \
	python3 build_parent_sample.py $(DATA_DIR)/snls $(ASTROPILE_ROOT)/snls --dirty; \
	cp snls.py $(ASTROPILE_ROOT)/snls/snls.py; \
	cd ..
snls: $(ASTROPILE_ROOT)/snls

# create parent sample for foundation
$(ASTROPILE_ROOT)/foundation:
	cd foundation; \
	mkdir -p $(DATA_DIR)/foundation; \
	mkdir -p $(ASTROPILE_ROOT)/foundation; \
	python3 download_data.py --destination_path $(DATA_DIR)/foundation; \
	python3 build_parent_sample.py $(DATA_DIR)/foundation $(ASTROPILE_ROOT)/foundation --dirty; \
	cp foundation.py $(ASTROPILE_ROOT)/foundation/foundation.py; \
	cd ..
foundation: $(ASTROPILE_ROOT)/foundation


# create parent sample for btsbot
$(ASTROPILE_ROOT)/btsbot:
	cd btsbot; \
	mkdir -p $(DATA_DIR)/btsbot; \
	mkdir -p $(ASTROPILE_ROOT)/btsbot; \
	python3 download_data.py $(DATA_DIR)/btsbot; \
	python3 build_parent_sample.py $(DATA_DIR)/btsbot $(ASTROPILE_ROOT)/btsbot --dirty; \
	cp btsbot.py $(ASTROPILE_ROOT)/btsbot/btsbot.py; \
	cd ..
btsbot: $(ASTROPILE_ROOT)/btsbot


# Create parent sample for MaNGA
# Note that this requires first manually downloading the MaNGA data from the SDSS website
$(ASTROPILE_ROOT)/manga:
	cd manga; \
	python build_parent_sample.py \
		--manga_data_path $(DATA_DIR)/manga \
		--output_dir $(ASTROPILE_ROOT)/manga \
		--max_samples_per_file 64 \
		--num_processes 8
	cp manga.py $(ASTROPILE_ROOT)/manga/manga.py; \
	cd ..
manga: $(ASTROPILE_ROOT)/manga


# create parent sample for tess
$(ASTROPILE_ROOT)/tess:
	cd tess; \
	mkdir -p $(DATA_DIR)/tess; \
	mkdir -p $(ASTROPILE_ROOT)/tess; \
	python3 mast_s3_transfer.py $(DATA_DIR)/tess; \
	python3 build_parent_sample.py $(DATA_DIR)/tess $(ASTROPILE_ROOT)/tess --num_processes 20; \
	cp tess.py $(ASTROPILE_ROOT)/tess/tess.py; \
	cd ..
tess: $(ASTROPILE_ROOT)/tess

# Create parent sample for Chandra
# Note that this requires first manually downloading the Chandra data 
# And untaring it, see the instuction in the readme file. It should look like that
# python download_script.py --min_cnts 40 --min_sig 4 --max_theta 10 --output_file catalog --file_path $DATA_DIR/chandra
$(ASTROPILE_ROOT)/chandra:
	cd chandra; \
	python build_parent_sample.py --cat_file catalog.hdf5 --output_path $(ASTROPILE_ROOT)/chandra/spectra  --file_path $(DATA_DIR)/chandra/ --num_workers 32
	cp chandra.py $(ASTROPILE_ROOT)/chandra/chandra.py; \
	cd ..
chandra: $(ASTROPILE_ROOT)/chandra
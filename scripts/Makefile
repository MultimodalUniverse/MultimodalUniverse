MMU_ROOT := /mnt/ceph/users/polymathic/MultimodalUniverse
DATA_DIR := /mnt/ceph/users/polymathic/external_data/astro

.PHONY: all apogee desi sdss decals plasticc jwst gz10 hsc vipers gaia desi_provabgs yse des_y3_sne_ia swift_sne_ia ps1_sne_ia snls foundation btsbot manga tess

all: apogee desi sdss decals plasticc jwst gz10 vipers hsc gaia desi_provabgs yse des_y3_sne_ia swift_sne_ia ps1_sne_ia snls foundation btsbot manga tess

# Create the parent sample for APOGEE
$(MMU_ROOT)/apogee:
	cd apogee; \
	python build_parent_sample.py --apogee_data_path=$(DATA_DIR)/APOGEE \
	                              --output_dir=$(MMU_ROOT)/apogee \
	                              --num_processes 32; \
    cp apogee.py $(MMU_ROOT)/apogee/apogee.py; \
	cd ..
apogee: $(MMU_ROOT)/apogee

# Create the parent sample for desi
$(MMU_ROOT)/desi:
	cd desi; \
	python build_parent_sample.py $(DATA_DIR)/DESI \
	                              $(MMU_ROOT)/desi \
	                              --num_processes 32; \
	cp desi.py $(MMU_ROOT)/desi/desi.py; \
	cd ..
desi: $(MMU_ROOT)/desi

# Create the parent sample for SDSS
$(MMU_ROOT)/sdss:
	cd sdss; \
	python build_parent_sample.py $(DATA_DIR)/SDSS \
	                              $(MMU_ROOT)/sdss \
	                              --num_processes 32; \
	cp sdss.py $(MMU_ROOT)/sdss/sdss.py; \
	cd ..
sdss: $(MMU_ROOT)/sdss

# Create the parent sample for ssl_legacysurvey
$(MMU_ROOT)/ssl_legacysurvey:
	cd ssl_legacysurvey; \
	python build_parent_sample.py $(DATA_DIR)/DECALS_Stein_et_al \
	                              $(MMU_ROOT)/decals \
	                              --num_processes 32; \
	cp ssl_legacysurvey.py $(MMU_ROOT)/ssl_legacysurvey/ssl_legacysurvey.py; \
	cd ..
ssl_legacysurvey: $(MMU_ROOT)/ssl_legacysurvey

# Create the parent sample for plasticc
$(MMU_ROOT)/plasticc:
	cd plasticc; \
    python build_parent_sample.py $(DATA_DIR)/PLAsTiCC \
                                  $(MMU_ROOT)/plasticc \
                                  --num_processes 32; \
    cp plasticc.py $(MMU_ROOT)/plasticc/plasticc.py; \
    cd ..
plasticc: $(MMU_ROOT)/plasticc

# Create the parent sample for jwst
$(MMU_ROOT)/jwst:
	cd jwst; \
	mkdir -p $(DATA_DIR)/JWST; \
	for survey in ngdeep primer-cosmos-east primer-uds gds gdn ceers-full; do \
		python build_parent_sample.py $$survey --image_dir=$(DATA_DIR)/JWST \
			--output_dir=$(ASTROPILE_ROOT)/jwst; \
	done; \
	cp jwst.py $(ASTROPILE_ROOT)/jwst/jwst.py; \
	cd ..
jwst: $(MMU_ROOT)/jwst

# Create the parent sample for GZ10 classification data
$(MMU_ROOT)/gz10:
	cd gz10; \
	python build_parent_sample.py $(DATA_DIR)/Galaxy10_DECals.h5 \
	                              $(MMU_ROOT)/gz10; \
	cp gz10.py $(MMU_ROOT)/gz10/gz10.py; \
	cd ..
gz10: $(MMU_ROOT)/gz10

# Create parent sample for vipers
$(MMU_ROOT)/vipers:
	cd vipers; \
	python build_parent_sample.py $(MMU_ROOT)/vipers \
                                  --num_processes 32; \
	cp vipers.py $(MMU_ROOT)/vipers/vipers.py; \
	cd ..
vipers: $(MMU_ROOT)/vipers

# Create parent sample for hsc
$(MMU_ROOT)/hsc:
	cd hsc; \
	python build_parent_sample.py pdr3_dud_22.5.sql $(MMU_ROOT)/hsc --rerun pdr3_dud --num_processes 1;\
	cp hsc.py $(MMU_ROOT)/hsc/hsc.py; \
	cd ..
hsc: $(MMU_ROOT)/hsc

# create parent sample for Gaia
$(MMU_ROOT)/gaia:
	cd gaia; \
	mkdir -p $(DATA_DIR)/Gaia; \
	mkdir -p $(MMU_ROOT)/gaia; \
	python3 download_parts.py --aria2 --output_dir $(DATA_DIR)/Gaia; \
	python3 merge_parts.py --input_dir $(DATA_DIR)/Gaia --output_file $(DATA_DIR)/Gaia/merged.hdf5; \
	python3 healpixify.py --input_file $(DATA_DIR)/Gaia/merged.hdf5 --output_dir $(MMU_ROOT)/gaia --nside 16 --num_procs 1; \
	cp gaia.py $(MMU_ROOT)/gaia/gaia.py; \
	cd ..
gaia: $(MMU_ROOT)/gaia

# create parent sample for desi_provabgs
$(MMU_ROOT)/desi_provabgs:
	cd desi_provabgs; \
	mkdir -p $(DATA_DIR)/DESI_PROVABGS; \
	mkdir -p $(MMU_ROOT)/desi_provabgs; \
	python3 build_parent_sample.py $(DATA_DIR)/DESI_PROVABGS $(MMU_ROOT)/desi_provabgs; \
	cp desi_provabgs.py $(MMU_ROOT)/desi_provabgs/desi_provabgs.py; \
	cd ..
desi_provabgs: $(MMU_ROOT)/desi_provabgs


# create parent sample for yse
$(MMU_ROOT)/yse:
	cd yse; \
	mkdir -p $(DATA_DIR)/yse; \
	mkdir -p $(MMU_ROOT)/yse; \
	python3 download_data.py --destination_path $(DATA_DIR)/yse; \
	python3 build_parent_sample.py --yse_data_path $(DATA_DIR)/yse --output_dir $(MMU_ROOT)/yse --dirty; \
	cp yse.py $(MMU_ROOT)/yse/yse.py; \
	cd ..
yse: $(MMU_ROOT)/yse


# create parent sample for des_y3_sne_ia
$(MMU_ROOT)/des_y3_sne_ia:
	cd des_y3_sne_ia; \
	mkdir -p $(DATA_DIR)/des_y3_sne_ia; \
	mkdir -p $(MMU_ROOT)/des_y3_sne_ia; \
	python3 download_data.py --destination_path $(DATA_DIR)/des_y3_sne_ia; \
	python3 build_parent_sample.py $(DATA_DIR)/des_y3_sne_ia $(MMU_ROOT)/des_y3_sne_ia --dirty; \
	cp des_y3_sne_ia.py $(MMU_ROOT)/des_y3_sne_ia/des_y3_sne_ia.py; \
	cd ..
des_y3_sne_ia: $(MMU_ROOT)/des_y3_sne_ia


# create parent sample for swift_sne_ia
$(MMU_ROOT)/swift_sne_ia:
	cd swift_sne_ia; \
	mkdir -p $(DATA_DIR)/swift_sne_ia; \
	mkdir -p $(MMU_ROOT)/swift_sne_ia; \
	python3 download_data.py --destination_path $(DATA_DIR)/swift_sne_ia; \
	python3 build_parent_sample.py $(DATA_DIR)/swift_sne_ia $(MMU_ROOT)/swift_sne_ia --dirty; \
	cp swift_sne_ia.py $(MMU_ROOT)/swift_sne_ia/swift_sne_ia.py; \
	cd ..
swift_sne_ia: $(MMU_ROOT)/swift_sne_ia

# create parent sample for ps1_sne_ia
$(MMU_ROOT)/ps1_sne_ia:
	cd ps1_sne_ia; \
	mkdir -p $(DATA_DIR)/ps1_sne_ia; \
	mkdir -p $(MMU_ROOT)/ps1_sne_ia; \
	python3 download_data.py --destination_path $(DATA_DIR)/ps1_sne_ia; \
	python3 build_parent_sample.py $(DATA_DIR)/ps1_sne_ia $(MMU_ROOT)/ps1_sne_ia --dirty; \
	cp ps1_sne_ia.py $(MMU_ROOT)/ps1_sne_ia/ps1_sne_ia.py; \
	cd ..
ps1_sne_ia: $(MMU_ROOT)/ps1_sne_ia

# create parent sample for snls
$(MMU_ROOT)/snls:
	cd snls; \
	mkdir -p $(DATA_DIR)/snls; \
	mkdir -p $(MMU_ROOT)/snls; \
	python3 download_data.py --destination_path $(DATA_DIR)/snls; \
	python3 build_parent_sample.py $(DATA_DIR)/snls $(MMU_ROOT)/snls --dirty; \
	cp snls.py $(MMU_ROOT)/snls/snls.py; \
	cd ..
snls: $(MMU_ROOT)/snls

# create parent sample for foundation
$(MMU_ROOT)/foundation:
	cd foundation; \
	mkdir -p $(DATA_DIR)/foundation; \
	mkdir -p $(MMU_ROOT)/foundation; \
	python3 download_data.py --destination_path $(DATA_DIR)/foundation; \
	python3 build_parent_sample.py $(DATA_DIR)/foundation $(MMU_ROOT)/foundation --dirty; \
	cp foundation.py $(MMU_ROOT)/foundation/foundation.py; \
	cd ..
foundation: $(MMU_ROOT)/foundation


# create parent sample for btsbot
$(MMU_ROOT)/btsbot:
	cd btsbot; \
	mkdir -p $(DATA_DIR)/btsbot; \
	mkdir -p $(MMU_ROOT)/btsbot; \
	python3 download_data.py $(DATA_DIR)/btsbot; \
	python3 build_parent_sample.py $(DATA_DIR)/btsbot $(MMU_ROOT)/btsbot --dirty; \
	cp btsbot.py $(MMU_ROOT)/btsbot/btsbot.py; \
	cd ..
btsbot: $(MMU_ROOT)/btsbot


# Create parent sample for MaNGA
# Note that this requires first manually downloading the MaNGA data from the SDSS website
$(MMU_ROOT)/manga:
	cd manga; \
	python build_parent_sample.py \
		--manga_data_path $(DATA_DIR)/manga \
		--output_dir $(ASTROPILE_ROOT)/manga \
		--max_samples_per_file 64 \
		--num_processes 8
	cp manga.py $(ASTROPILE_ROOT)/manga/manga.py; \
	cd ..
manga: $(MMU_ROOT)/manga


# create parent sample for tess
$(MMU_ROOT)/tess:
	cd tess; \
	mkdir -p $(DATA_DIR)/tess; \
	mkdir -p $(MMU_ROOT)/tess; \
	python3 mast_s3_transfer.py $(DATA_DIR)/tess; \
	python3 build_parent_sample.py $(DATA_DIR)/tess $(MMU_ROOT)/tess --num_processes 20; \
	cp tess.py $(MMU_ROOT)/tess/tess.py; \
	cd ..
tess: $(MMU_ROOT)/tess

# Create parent sample for Chandra
# Note that this requires first manually downloading the Chandra data 
# And untaring it, see the instuction in the readme file. It should look like that
# python download_script.py --min_cnts 40 --min_sig 4 --max_theta 10 --output_file catalog --file_path $DATA_DIR/chandra
$(MMU_ROOT)/chandra:
	cd chandra; \
	python build_parent_sample.py --cat_file catalog.hdf5 --output_path $(MMU_ROOT)/chandra/spectra  --file_path $(DATA_DIR)/chandra/ --num_workers 32
	cp chandra.py $(MMU_ROOT)/chandra/chandra.py; \
	cd ..
chandra: $(MMU_ROOT)/chandra

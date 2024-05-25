#!/bin/bash

export ASTROPILE_ROOT='/mnt/ceph/users/polymathic/AstroPile'
export DATA_DIR='/mnt/ceph/users/polymathic/data'

# Compile APOGEE dataset if the apogee folder does not exist yet in
# output ASTROPILE_ROOT directory
if [ ! -d "$ASTROPILE_ROOT/apogee" ]; then
    # Change directory to the apogee folder
    cd apogee
    
    # Execute the build_parent_sample.py script
    python build_parent_sample.py --apogee_data_path=$DATA_DIR/apogee \
                                  --output_dir=$ASTROPILE_ROOT \
                                  --num_processes 32
    
    # Change directory back to the parent directory
    cd ..
fi


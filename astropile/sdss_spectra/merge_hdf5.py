import os
import h5py

# Directory containing input H5 files
input_directory = '/mnt/ceph/users/lparker/sdss_spectra'

# List of input H5 files
input_files = [os.path.join(input_directory, f'{i}.h5') for i in range(9)]

# Output merged H5 file
output_file = os.path.join(input_directory, 'merged.h5')

# Create a new HDF5 file for merging
with h5py.File(output_file, 'w') as merged_h5:

    # Iterate through each input file and copy its contents into a group
    for input_file in input_files:
        print(f'Working on file {input_file}')
        with h5py.File(input_file, 'r') as input_h5:
            # Extract the group name from the input file name (without the '.h5' extension)
            group_name = os.path.basename(input_file)[:-3]
            
            # Create a new group in the merged file
            group = merged_h5.create_group(group_name)
            
            # Copy all datasets and attributes from the input file to the group
            for name, item in input_h5.items():
                input_h5.copy(name, group)

print(f'Merged H5 file created: {output_file}')
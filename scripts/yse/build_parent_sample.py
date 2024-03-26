import os
import argparse

# Import sn specific packages
import sncomso

def main(args):
    
    for fn in os.listdir(args.yse_data_path):

        # Read snana data
        meta, table = sncosmo.read_snana_ascii(args.yse_data_path+fn, default_tablename='OBS')

        



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Extract ... and convert to standard time-series data format')
    parser.add_argument('yse_data_path', type=str, help='Path to the local copy of the YSE DR1 data')
    parser.add_argument('output_dir', type=str, help='Path to the output directory')
    args = parser.parse_args()

    main(args)
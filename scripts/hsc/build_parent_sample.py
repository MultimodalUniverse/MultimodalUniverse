import os
import argparse
from astropy.table import Table
import astropy.units as u
from unagi import hsc, task

HSC_PIXEL_SCALE = 0.168 # Size of a pixel in arcseconds

filters = ['HSC-G', 'HSC-R', 'HSC-I', 'HSC-Z', 'HSC-Y']


def main(args):

    # Create the output directory if it does not exist
    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)

    # Login to the HSC archive
    archive = hsc.Hsc(dr=args.dr, rerun=args.rerun)

    # Define the filename for the output catalog resulting from the query 
    catalog_filename = os.path.join(args.output_dir, args.query_file.split('/')[-1].split('.sql')[0]+'.fits')
                                    
    # Check if the result of the query already exists
    if not os.path.exists(catalog_filename):
        print("Running query...")

        # Run the query
        catalog = archive.sql_query(args.query_file, from_file=True, out_file=catalog_filename)

        print("query saved to {}".format(catalog_filename))
    else:
        # Read the catalog from disk
        catalog = Table.read(catalog_filename)
        print("catalog loaded from {}".format(catalog_filename))

    cutout_size = HSC_PIXEL_SCALE*(args.cutout_size//2 + 2.5) # Size of cutouts in arcsecs, with some additional margin

    # Create a temporary directory for the cutouts
    tmp_dir = os.path.join(args.temp_dir, 'hsc_cutouts')
    if not os.path.exists(tmp_dir):
        os.makedirs(tmp_dir)

    # Now that the catalog has been created, we can download the cutouts for each object
    cutouts_filename = task.hsc_bulk_cutout(catalog, 
                                        cutout_size=cutout_size* u.Unit('arcsec'), 
                                        filters=filters, 
                                        archive=archive,  
                                        nproc=args.num_processes, 
                                        tmp_dir=tmp_dir, 
                                        output_dir=args.output_dir)
    
    print("Dataset saved to {}".format(cutouts_filename))

    # Remove the temporary directory
    os.rmdir(tmp_dir)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Takes an SQL query file, runs the query, and returns the cutouts for the objects in the query')
    parser.add_argument('query_file', type=str, help='The path to the SQL query file')
    parser.add_argument('output_dir', type=str, help='The path to the output directory')
    parser.add_argument('--temp_dir', type=str, default='/tmp', help='The path to the temporary download directory')
    parser.add_argument('--cutout_size', type=int, default=224, help='The size of the cutouts to download')
    parser.add_argument('--num_processes', type=int, default=4, help='The number of processes to use for parallel processing (maximum 4)')
    parser.add_argument('--rerun', type=str, default='pdr3_wide', help='The rerun to use')
    parser.add_argument('--dr', type=str, default='pdr3', help='The data release to use')
    args = parser.parse_args()
    main(args)

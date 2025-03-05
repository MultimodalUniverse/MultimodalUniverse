#!/bin/bash
#SBATCH --job-name=tess_spoc_dl
#SBATCH --output=tess_spoc_dl_%j.log
#SBATCH --error=tess_spoc_dl_%j.err
#SBATCH --time=72:00:00
#SBATCH --mem=1000G
#SBATCH --cpus-per-task=64
#SBATCH --mail-type=BEGIN,END,FAIL

# Configuration
DATA_PATH="/mnt/ceph/users/polymathic/MultimodalUniverse/"  # Change this to your data directory
PIPELINE="spoc"
N_PROCESSES=64

# Parse command line arguments
RESTART=false
TEST_ONLY=false

while [[ $# -gt 0 ]]; do
  case $1 in
    --restart)
      RESTART=true
      shift
      ;;
    --test-only)
      TEST_ONLY=true
      shift
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

# Load necessary modules (adjust for your environment)
# module load python/3.10
# module load astropy

# Activate virtual environment if needed
source ~/envs/mmu/bin/activate

echo "Starting TESS SPOC download job at $(date)"
echo "Data path: $DATA_PATH"
echo "Pipeline: $PIPELINE"
echo "Processes: $N_PROCESSES"
echo "Restart mode: $RESTART"
echo "Test only: $TEST_ONLY"

# Create directories if they don't exist
mkdir -p $DATA_PATH
mkdir -p $DATA_PATH/fits

# Set up common parameters
COMMON_ARGS="--pipeline $PIPELINE --data_path $DATA_PATH --hdf5_output_path $DATA_PATH --fits_output_path $DATA_PATH/fits --n_processes $N_PROCESSES"

# Add restart flag if needed
if [ "$RESTART" = true ]; then
    COMMON_ARGS="$COMMON_ARGS --resume --skip_existing"
    echo "Running in restart mode - will skip completed sectors"
fi

# First, run a test with sector 1 to make sure everything works
echo "Running test download with sector 1..."
python build_parent_sample.py -s 1 $COMMON_ARGS

# Check if the test was successful
if [ $? -ne 0 ]; then
    echo "Test download failed! Exiting."
    exit 1
fi

echo "Test download successful!"

# If test-only flag is set, exit here
if [ "$TEST_ONLY" = true ]; then
    echo "Test-only mode enabled. Exiting after successful test."
    exit 0
fi

# List completed sectors before starting the full download
echo "Currently completed sectors:"
python list_sectors.py --db_path $DATA_PATH/tess_downloads.db --pipeline $PIPELINE

# Download all remaining sectors
echo "Starting download of all SPOC sectors at $(date)"
python build_parent_sample.py -s all $COMMON_ARGS

# Check final status
echo "Download job completed at $(date)"
echo "Final sector status:"
python list_sectors.py --db_path $DATA_PATH/tess_downloads.db --pipeline $PIPELINE

# Check for any failed downloads
python check_failures.py $DATA_PATH/tess_downloads.db

echo "Job complete!" 
#!/bin/bash
#SBATCH --job-name=lamost-proc
#SBATCH --partition=cca
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=100G
#SBATCH --time=7-00:00:00
#SBATCH --output=logs/lamost-proc_%A_%a.out
#SBATCH --error=logs/lamost-proc_%A_%a.err
#SBATCH --array=0-16

# Usage:
#   mkdir -p logs
#   sbatch process_tars.sbatch
#
# Adjust --array=0-N to set the number of workers (world_size = N+1).
# Each array task processes a disjoint shard of tar.gz files into
# intermediate HDF5s. Run build_parent_sample.py after all tasks complete.

WORLD_SIZE=$(( SLURM_ARRAY_TASK_MAX + 1 ))
RANK=$SLURM_ARRAY_TASK_ID

export PATH="/mnt/home/jshen/local/bin:$PATH"

VENV_DIR="/mnt/home/jshen/venvs/gpt/bin/activate"
CATALOG="/mnt/home/polymathic/ceph/external_data/astro/lamost/dr11_v2.0_LRS_catalogue.fits"
TAR_DIR="/mnt/home/polymathic/ceph/external_data/astro/lamost"
OUTPUT_DIR="/mnt/home/polymathic/ceph/external_data/astro/lamost"

source $VENV_DIR

echo "=== LAMOST Process Tars ==="
echo "Rank: ${RANK} / World size: ${WORLD_SIZE}"
echo "Host: $(hostname)"
echo "Date: $(date)"
echo "Catalog: ${CATALOG}"
echo "Tar dir: ${TAR_DIR}"
echo "Output dir: ${OUTPUT_DIR}"
echo "==========================="

srun python3 process_tars.py \
    "${CATALOG}" \
    "${TAR_DIR}" \
    "${OUTPUT_DIR}" \
    --rank "${RANK}" \
    --world_size "${WORLD_SIZE}"

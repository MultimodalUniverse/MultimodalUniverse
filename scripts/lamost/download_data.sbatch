#!/bin/bash
#SBATCH --job-name=lamost-dl
#SBATCH --partition=cca
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=100G
#SBATCH --time=7-00:00:00
#SBATCH --output=logs/lamost-dl_%A_%a.out
#SBATCH --error=logs/lamost-dl_%A_%a.err
#SBATCH --array=0-16

# Usage:
#   mkdir -p logs
#   sbatch download_data.sbatch
#
# Adjust --array=0-N to set the number of nodes (world_size = N+1).
# Each array task downloads a disjoint shard of observation-date tar.gz files.
# Tar files are kept as-is (not extracted) for use by build_parent_sample.py.

WORLD_SIZE=$(( SLURM_ARRAY_TASK_MAX + 1 ))
RANK=$SLURM_ARRAY_TASK_ID

export PATH="/mnt/home/jshen/local/bin:$PATH"

VENV_DIR="/mnt/home/jshen/venvs/gpt/bin/activate"
OUTPUT_DIR="/mnt/home/polymathic/ceph/external_data/astro/lamost"

source $VENV_DIR

echo "=== LAMOST Download ==="
echo "Rank: ${RANK} / World size: ${WORLD_SIZE}"
echo "Host: $(hostname)"
echo "Date: $(date)"
echo "Output dir: ${OUTPUT_DIR}"
echo "========================"

srun python3 download_data.py \
    --catalog_name LRS_catalogue \
    --release dr11_v2.0 \
    -o "${OUTPUT_DIR}" \
    --use_aria2 \
    --rank "${RANK}" \
    --world_size "${WORLD_SIZE}"

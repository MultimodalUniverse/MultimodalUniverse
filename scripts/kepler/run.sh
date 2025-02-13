#!/bin/bash 
#SBATCH --job-name=Multimodal-kepler 
#SBATCH --output=logs/multimodal.%A.%a.out.txt
#SBATCH --error=logs/multimodal.%A.%a.err.txt
#SBATCH --partition=ph_hagai
#SBATCH --ntasks=1            # Number of MPI ranks (spus)
##SBATCH --cpus-per-task=192      # Number of OpenMP threads for each MPI process/rank
##SBATCH --nodes=1              # Number of nodes
#SBATCH --nodes=1
##SBATCH --mail-type=ALL
##SBATCH --mail-user=ilay.kamai@campus.technion.ac.il

export MASTER_PORT=$(expr 10000 + $(echo -n $SLURM_JOBID | tail -c 4))
export WORLD_SIZE=$(($SLURM_NNODES * $SLURM_NTASKS_PER_NODE))

source /usr/local/ph_hagai/anaconda3/bin/activate astro

python build_parent_sample.py /storage/ph_hagai/ilayk/kepler_data/kepler_dr25_catalog.csv \
 /storage/ph_hagai/ilayk/kepler_data/fits_data /storage/ph_hagai/ilayk/MultimodalUniverse/scripts/kepler/kepler_data -nproc=192
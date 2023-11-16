#!/bin/bash -l
#SBATCH -J sdss         # create a short name for your job
#SBATCH --time=24:00:00          # total run time limit (HH:MM:SS)
#SBATCH -p gen
#SBATCH -c 100
#SBATCH -o full.out

module purge
module load python
source /mnt/home/lparker/python_envs/sdss/bin/activate

python spectra07.py


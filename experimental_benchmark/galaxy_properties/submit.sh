#!/bin/bash -l

#SBATCH -p gpu
#SBATCH -t 48:00:00
#SBATCH -C a100,ib
#SBATCH -N 1
#SBATCH --gpus=1
#SBATCH --tasks-per-node=1
#SBATCH --cpus-per-task=12
#SBATCH --output=logs/supervised-%j.log

module purge
module load python
module load cuda
module load gcc

source /mnt/home/lparker/python_envs/toto/bin/activate

python trainer.py fit --config configs/spectrum.yaml


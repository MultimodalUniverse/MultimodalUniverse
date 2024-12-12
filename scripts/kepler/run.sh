#SBATCH --job-name=Multimodal-kepler  # create a short name for your job
#SBATCH --nodes=1                # node count
#SBATCH --ntasks-per-node=1      # total number of tasks per node
#SBATCH --cpus-per-task=96        # cpu-cores per task (>1 if multi-threaded tasks)
#SBATCH --gpus-per-node=1     # number of gpus per node          
#SBATCH --mail-type=begin        # send email when job begins
#SBATCH --mail-type=end          # send email when job ends
#SBATCH --mail-user=ilay.kamai@campus.technion.ac.il
#SBATCH --qos=basic
export MASTER_PORT=$(expr 10000 + $(echo -n $SLURM_JOBID | tail -c 4))
export WORLD_SIZE=$(($SLURM_NNODES * $SLURM_NTASKS_PER_NODE))

master_addr=$(scontrol show hostnames "$SLURM_JOB_NODELIST" | head -n 1)
export MASTER_ADDR=$master_addr

srun --container-image="./containers/lightPred4.sqsh" --output=logs/multimodal_kepler_${i}_%J.out \
         --error=logs/multimodal_kepler_${i}_%J.err  --no-container-entrypoint --container-mounts=./work:/data \
          python /data/MultimodalUniverse/scripts/kepler/build_parent_sample.py /data/lightPred/tables /data/MultimodalUniverse/scripts/kepler/data -nproc=$SLURM_CPUS_PER_NODE
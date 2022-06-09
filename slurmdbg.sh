#!/bin/bash
#SBATCH -n 1
#SBATCH -p debug
#SBATCH --cpus-per-task=4
#SBATCH --mem=8G
#SBATCH --time=00:30:00
#SBATCH --output /tmp/debug-%j.log

export PYTHONUNBUFFERED=TRUE
export PYTHONPATH=./:$PYTHONPATH

echo "Establishing connection back to $SLURM_SUBMIT_HOST:45589"
SHORTTEST=true python -m debugpy --connect $SLURM_SUBMIT_HOST:45589 --wait-for-client tests/test_EventsAggregator.py
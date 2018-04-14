#!/bin/bash
#SBATCH -p physical
#SBATCH --time=01:00:00
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=4
#SBATCH --output=result.out

module load Python/2.7.10-goolf-2015a

time mpiexec python ass1.py
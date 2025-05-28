#!/usr/bin/env /bin/bash

#SBATCH -A prepp-water
#SBATCH -N 1
#SBATCH -t 03:00:00
#SBATCH -p short
#SBATCH --job-name=rev-solar-2023-2024
#SBATCH --mail-user=cameron.bracken@pnnl.gov

echo "rev solar points hist 2023 2024"

source ~/venv/rev/bin/activate

INPUT_DIR=/rcfs/projects/im3/data/solar-wind/met_data/historical/
OUTPUT_DIR=/people/brac840/tgw-gen/gen
CONFIG_SOLAR=sam/configs/eia_solar_configs.csv

echo "python rev_solar.py points 2023 $INPUT_DIR $OUTPUT_DIR $CONFIG_SOLAR"

srun python rev_solar.py points 2023 $INPUT_DIR $OUTPUT_DIR $CONFIG_SOLAR &
wait
srun python rev_solar.py points 2024 $INPUT_DIR $OUTPUT_DIR $CONFIG_SOLAR &
wait

echo "Done"

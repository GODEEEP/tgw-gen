#!/usr/bin/env /bin/bash

#SBATCH -A eo-ra
#SBATCH -N 1
#SBATCH -t 03:00:00
#SBATCH -p short
#SBATCH --job-name=rev-wind-2023-2024
#SBATCH --mail-user=cameron.bracken@pnnl.gov

echo "rev wind points hist 2023 2024"

source ~/venv/rev/bin/activate

INPUT_DIR=/rcfs/projects/im3/data/solar-wind/met_data/historical/
OUTPUT_DIR=/people/brac840/tgw-gen/gen
CONFIG_WIND=sam/configs/eia_wind_configs.csv

echo "python rev_wind.py points 2023 $INPUT_DIR $OUTPUT_DIR $CONFIG_WIND"

srun python rev_wind.py points 2023 $INPUT_DIR $OUTPUT_DIR $CONFIG_WIND &
wait
srun python rev_wind.py points 2024 $INPUT_DIR $OUTPUT_DIR $CONFIG_WIND &
wait

echo "Done"

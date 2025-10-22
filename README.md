# UFC ELO Calculator
UFC ELO Calculator service for computing and exposing fighter ELO ratings.

## Seeder data

This project seeds initial data (promotions, events) from CSV files. By default, the Docker Compose setup mounts a host directory into the container at `/app/seeder_data`:

- Compose volume: `../seeder_data:/app/seeder_data:ro`

This assumes your seeder CSVs are located at a `seeder_data/` folder adjacent to the `ufc-elo-calculator/` project directory. If your directory structure differs, either:

- Place the CSVs under `seeder_data/` next to the project root, or
- Set the `SEEDER_DATA_DIR` environment variable to an absolute path containing `events.csv` and `promotions.csv`.

At startup, the app looks for seeder files in this order:

1. `SEEDER_DATA_DIR` (env override)
2. `/app/seeder_data` (container default)
3. `./seeder_data` (current working directory)

Missing directories or files are logged as warnings and seeding will be skipped for absent files (startup won’t fail).

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

## API endpoints

Base path: the FastAPI app exposes routes under `/ingest` for data ingestion and ELO processing.

- POST `/ingest/events`
	- Body: `{ "event_ids": [int, ...] }`
	- Seeds fighters and events for the provided event IDs.

- POST `/ingest/events-by-link`
	- Body: `{ "event_links": ["https://www.ufcstats.com/event-details/..."] }`
	- Seeds fighters and events by UFCStats event page links.

- POST `/ingest/events-elo`
	- Body: `{ "event_ids": [int, ...] }`
	- Recomputes ELO event-by-event based on existing bouts/participants for those events (no scraping).

- POST `/ingest/events-fights-elo`
	- Body: `{ "event_ids": [int, ...] }`
	- For each event ID: scrapes its UFCStats event page for fights, persists Bout and BoutParticipant data, then computes and persists ELO immediately after each fight. Continues across events even if a single event fails (warning is logged).

- POST `/ingest/events-first`
	- Body: `{ "limit": int }`
	- Seeds the first N unseeded events and their fighters, returning a summary.

Responses are wrapped in a `MainResponse[...]` envelope with `ok: true` and a typed `data` payload.

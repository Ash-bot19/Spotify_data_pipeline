# Spotify_data_pipeline
Free daily Spotify Top Artists data pipeline (ETL + dashboard)

## Setup

1. Install dependencies (inside a virtualenv is recommended):
   ```bash
   pip install -r ETL/requirements.txt
   ```
2. Create a `.env` file (either in the repo root or inside `ETL/`) with:
   ```env
   SPOTIFY_CLIENT_ID=your_spotify_app_client_id
   SPOTIFY_CLIENT_SECRET=your_spotify_app_client_secret
   # optional overrides:
   # SUPABASE_DATABASE_URL=postgres_connection_string
   # FILES_OUTPUT_DIR=absolute_or_relative_path_for_parquet_outputs
   ```
   Environment variables set in the shell take precedence; the `.env` is a convenience for local runs.

## Running the ETL

```bash
python -m ETL.main
```

By default the pipeline writes parquet snapshots to `ETL/outputs/`. Set `TARGET=postgres` to load into a database instead (requires `SUPABASE_DATABASE_URL`).

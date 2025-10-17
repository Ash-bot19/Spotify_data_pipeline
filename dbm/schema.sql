CREATE TABLE IF NOT EXISTS bronze_daily_tracks (
  snapshot_date date,
  market text,
  playlist_id text,
  playlist_name text,
  rank int,
  track_id text,
  track_name text,
  artist_ids text[],
  artist_names text[],
  score int,
  PRIMARY KEY (snapshot_date, market, rank)
);

CREATE TABLE IF NOT EXISTS silver_artist_market_daily (
  snapshot_date date,
  market text,
  artist_id text,
  artist_name text,
  tracks int,
  total_score int,
  best_rank int,
  PRIMARY KEY (snapshot_date, market, artist_id)
);

CREATE TABLE IF NOT EXISTS gold_artist_global_daily (
  snapshot_date date,
  artist_id text,
  artist_name text,
  markets int,
  total_score int,
  best_rank int,
  PRIMARY KEY (snapshot_date, artist_id)
);

CREATE INDEX IF NOT EXISTS idx_silver_date_market
  ON silver_artist_market_daily (snapshot_date, market);
CREATE INDEX IF NOT EXISTS idx_gold_date
  ON gold_artist_global_daily (snapshot_date);

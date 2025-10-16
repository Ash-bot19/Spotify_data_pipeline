import os
import pandas as pd
from sqlalchemy import create_engine, text

def upsert_df(df, table, engine, pkey_cols):
    if df.empty: return
    tmp = f"_{table}_stg"
    with engine.begin() as conn:
        df.head(0).to_sql(tmp, conn, index=False, if_exists="replace")
        df.to_sql(tmp, conn, index=False, if_exists="append")
        cols = ",".join(df.columns)
        # build ON CONFLICT upsert (Postgres)
        conflict = ",".join(pkey_cols)
        updates = ",".join([f"{c}=EXCLUDED.{c}" for c in df.columns if c not in pkey_cols])
        sql = f"""
        INSERT INTO {table} ({cols})
        SELECT {cols} FROM {tmp}
        ON CONFLICT ({conflict}) DO UPDATE SET {updates};
        DROP TABLE {tmp};
        """
        conn.execute(text(sql))

def load_to_postgres(bronze, silver, gold):
    engine = create_engine(os.environ["SUPABASE_DATABASE_URL"], pool_pre_ping=True)
    upsert_df(bronze, "bronze_daily_tracks", engine,
              ["snapshot_date","market","rank"])
    upsert_df(silver, "silver_artist_market_daily", engine,
              ["snapshot_date","market","artist_id"])
    upsert_df(gold, "gold_artist_global_daily", engine,
              ["snapshot_date","artist_id"])

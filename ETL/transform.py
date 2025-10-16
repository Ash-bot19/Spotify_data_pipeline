import pandas as pd

def to_artist_rankings(snapshots):
    df = pd.DataFrame(snapshots)
    if df.empty:
        return df, df, df

    # score = 51 - rank (so rank 1 = 50 pts, rank 50 = 1 pt)
    df["score"] = 51 - df["rank"]

    # explode artists because tracks can have multiple artists
    exploded = df.explode(["artist_ids", "artist_names"]).rename(
        columns={"artist_ids":"artist_id", "artist_names":"artist_name"}
    )

    # per market per day
    per_market = (exploded
        .groupby(["snapshot_date","market","artist_id","artist_name"], as_index=False)
        .agg(tracks=("track_id","nunique"),
             total_score=("score","sum"),
             best_rank=("rank","min"))
        .sort_values(["snapshot_date","market","total_score"], ascending=[True, True, False])
    )

    # global (across markets) per day
    global_daily = (per_market
        .groupby(["snapshot_date","artist_id","artist_name"], as_index=False)
        .agg(markets=("market","nunique"),
             total_score=("total_score","sum"),
             best_rank=("best_rank","min"))
        .sort_values(["snapshot_date","total_score"], ascending=[True, False])
    )
    return df, per_market, global_daily

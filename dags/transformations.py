import os
import pandas as pd
from datetime import datetime

date = datetime.today().strftime("%Y-%m-%d")
root_path = os.getcwd()


def transform_raw_data(output_loc):

    # read in data from `raw` folder
    df = pd.read_csv(f"{root_path}/filesystem/raw/retail_profiling-{date}.csv")

    # perform transformations here
    df["country"] = [None if x == "Unspecified" else x for x in df["country"]]
    df.dropna(subset=["customer_id"], inplace=True)
    df["customer_id"] = pd.to_numeric(df["customer_id"], downcast="integer")

    # load transformed data to `stage` folder
    df.to_parquet(output_loc)


def transform_stage_data(output_loc):

    # read in parquet file
    df = pd.read_parquet(
        f"{root_path}/filesystem/stage/retail_profiling-{date}.snappy.parquet"
    )

    # transform to CSV file because Postgres cannot copy parquet files
    df.to_csv(output_loc, index=False)

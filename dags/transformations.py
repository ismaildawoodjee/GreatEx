import os
import pandas as pd
from datetime import datetime

date = datetime.today().strftime("%Y-%m-%d")
root_path = os.getcwd()  # you have to currently be in the GreatEx root folder


def transform_raw_data(output_loc):

    # read in data from `raw` folder
    df = pd.read_csv(f"{root_path}/filesystem/raw/retail_profiling-{date}.csv")

    # perform transformations here
    df["country"] = [None if x == "Unspecified" else x for x in df["country"]]

    # load transformed data to `stage` folder
    df.to_parquet(output_loc)

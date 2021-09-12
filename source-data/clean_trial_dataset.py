"""Script for cleaning the trial dataset `retail_validating.csv` after
renaming it to `retail_profiling.csv`. The original `retail_profiling.csv` file
should be renamed to `retail_profiling_orig.csv`.
"""
import pandas as pd


def clean_data():
    df = pd.read_csv("retail_profiling.csv")

    df["Quantity"] = [0 if qty < 0 else qty for qty in df["Quantity"]]
    df["UnitPrice"] = [0.00 if price < 0 else price for price in df["UnitPrice"]]
    # Casting CustomerID to integer so that it fits the INT type in the source database table
    df["CustomerID"] = df["CustomerID"].astype("Int64")
    df["Country"] = [
        "United Kingdom" if country == "United KingdomKingdom" else country
        for country in df["Country"]
    ]

    df.to_csv("retail_profiling.csv", index=False)


if __name__ == "__main__":
    clean_data()

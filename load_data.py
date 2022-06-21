import pandas as pd
import os
datafiles = [file for file in os.listdir("amfidata2") if file.endswith(".csv")]

def merge_csvs(datafiles):
    end_df = pd.DataFrame()
    for file in datafiles:
         f = "amfidata2/" + file
         print(f"processing {f}")
         new_col = file.strip("_data.csv")
         new_df = pd.read_csv(f, index_col=0, parse_dates=True,
                             low_memory=False,
                             names=['Date', new_col], header=0 )
         if not end_df.empty:
             end_df = end_df.merge(new_df, on="Date", how="outer")
         else:
             end_df = new_df.copy()
    return end_df
all_csv = merge_csvs(datafiles)
all_csv.to_csv('all.csv')

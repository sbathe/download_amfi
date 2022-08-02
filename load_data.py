import pandas as pd
import json
import os
# list categories we are interested in
categories = [ 'Multi Cap Fund', 'Focused Fund', 'Multi Asset Allocation',
              'Value Fund', 'Flexi Cap Fund', 'Dividend Yield Fund',
              'Large & Mid Cap Fund', 'Large Cap Fund', 'Contra Fund']

category_data = json.load(open('amfidata2/categories.json'))
funds_list = []
for k in categories:
    funds_list.extend(category_data[k])

funds_list = list(dict.fromkeys(funds_list))
datafiles = [ f + '_data.csv' for f in funds_list ]


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
all_csv.to_csv('targeted_equity.csv')

from config import load_config
# import amc
# import datetime
from utils import utils
import pandas as pd
import os
import json
# import sys
from log import MyLog

log = MyLog(loggername="get-data")
logger, rootlogger = MyLog.setup_logging(log)


class Amcwriter:
    config = load_config()

    def write_schemewise_data(self,
                              data: json = None,
                              out_dir=config["storage_path"]):
        u = utils()
        for amc in data.keys():
            for scheme in data[amc].keys():
                # Get Scheme meta data
                meta_file = os.path.join(out_dir, scheme + "_meta.json")
                meta_data = data[amc][scheme]["meta"]
                # Write meta data
                u.write_json_to_file(filename=meta_file, json_data=meta_data)
                # Get scheme NAV Data
                data_file = os.path.join(out_dir, scheme + "_data.csv")
                data_data = data[amc][scheme]["data"]
                # Write NAV Data
                if os.path.isfile(data_file):
                    existing_data = pd.read_csv(data_file)
                else:
                    existing_data = pd.DataFrame()
                # new_csv_data =
                # pd.DataFrame.from_dict(data_data).drop(
                # columns='scheme_code').to_csv(index=0, index_label='date')
                new_csv_data = pd.DataFrame.from_dict(data_data).drop(
                    columns="scheme_code"
                )
                csv_data = pd.concat([existing_data, new_csv_data]).to_csv(
                    index=0, index_label="date"
                )
                u.write_file(filename=data_file, data=csv_data)

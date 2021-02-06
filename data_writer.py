from config import Config
import datetime
from utils import utils
import pandas as pd
import os
import json
import sys
from log import MyLog
log = MyLog(loggername='get-data')
logger, rootlogger = MyLog.setup_logging(log)
import amc

class Amcwriter:
    def __init__(self):
        self.config = Config.load_config(self)

    def write_schemewise_data(self, data=None, out_dir=None):
        if not out_dir:
            out_dir = self.config['storage_path']
        u = utils()   
        for amc in data.keys():
           for scheme in data[amc].keys():
               meta_file = os.path.join(out_dir,scheme + '_meta.json')
               meta_data = data[amc][scheme]['meta']
               data_file = os.path.join(out_dir,scheme + '_data.csv')
               data_data = data[amc][scheme]['data']
               u.write_json_to_file(filename=meta_file,json_data=meta_data)
               csv_data = pd.DataFrame.from_dict(data_data).drop(columns='scheme_code').to_csv(index=0, index_label='date')
               u.write_file(filename=data_file,data=csv_data)

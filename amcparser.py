from config import Config
import datetime
from utils import utils
import os
import json
import sys
import re
from log import MyLog
log = MyLog(loggername='get-data')
logger, rootlogger = MyLog.setup_logging(log)

class Parseamc:
    def __init__(self):
        self.NODATA = re.compile('No data found on the basis of selected parameters for this report')
        self.HEADING = re.compile('Scheme Code;Scheme Name')
        self.OPENENDED = re.compile('Open Ended Scheme')
        self.CLOSEENDED = re.compile('Close Ended Scheme')
        self.AMC = re.compile('Mutual Fund')
        self.config = Config.load_config(self)

    def nodata(self,raw_string):
        """ Checks if the data file has any useful info """
        if re.search(self.NODATA,raw_string):
            return True
        else:
            return False
    def write_file(self,fh,data):
        #Add some validations here
        try:
            fh.write(data)
            fh.close()
        except:
            logger.error("Unexpected error:{0}".format(sys.exc_info()[0]))

    def process_raw(self,raw_string):
        """ This will remove all blank lines from the downloaded file. Returns
        a list of non-blank lines"""
        return [ line for line in raw_string.split('\n') if line.strip() ]

    def parse(self,processed_data,data={}):
       """ The goal is:
           - Create a scheme code wise json structure with 2 sections:
           - meta: a dictionary that has scheme name, fund AMC, category etc
           - data: list of (date, NAV) tuples
       """
       for line in processed_data:
           if re.search(self.HEADING,line):
               _headings = line.split(';')
           elif re.search(self.OPENENDED,line):
              c = []
              parts = line.split('(')
              for p in parts:
                  c.append(p.split('-'))
                  categories = [item.strip(')').strip() for sublist in c for item in sublist]
           elif line.endswith('Mutual Fund'):
              amc = line
              if not amc in data.keys():
                data[amc] = {}
           elif len(line.split(';')) > 1:
               code,name,_ign1,_ign2,nav,_rp,_sp,date = line.split(';')
               if code not in data[amc].keys():
                 data[amc][code] = {}
                 data[amc][code]["data"] = []
                 data[amc][code]["meta"] = { "name": name, "amc": amc, "type": "open ended", "scheme_code": code, "categories": categories }
               #year = datetime.datetime.strptime(date,'%d-%b-%Y').year
               #if year in data[amc][code]["data"].keys():
                   #data[amc][code]["data"][year].append({"date": date,"nav": nav})
               #else:
               #    data[amc][code]["data"][year] = []
               #data[amc][code]["data"].append({"scheme_code": int(code), "date": str(datetime.datetime.strptime(date,'%d-%b-%Y')),"nav": nav})
               data[amc][code]["data"].append({"scheme_code": int(code), "date": date,"nav": nav})
       return data

    def get_json_from_amc_csvs(self,amcname=None,in_dir=None):
       if not in_dir: 
           in_dir = self.config['cache_path'] 
       filematch = '_'.join(amcname.split())
       amc_data = {}
       for _root, _dirs, files in os.walk(in_dir):
           for f in files:
               if not f.find(filematch):
                   d = open(os.path.join(in_dir,f)).read()
                   if not self.nodata(d):
                       pdata = self.process_raw(d)
                       amc_data = self.parse(pdata,amc_data)
       return amc_data
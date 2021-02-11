import requests
from config import load_config
import bs4
from log import MyLog
import sys
import json
import os
import datetime

log = MyLog(loggername='get-data')
logger, rootlogger = MyLog.setup_logging(log)
class utils:
    def __init__(self):
        self.CODES_URL = 'https://www.amfiindia.com/nav-history-download'
        self.config = load_config()

    def get_url_data(self,url):
        session = requests.Session()
        session.trust_env = False
        try:
            r = session.get(url,timeout=(5,300))
        except requests.exceptions.RequestException as e:
            logger.error('Cannot complete request to {0}. The error was:'.format(url))
            logger.debug(e)
            return None
        return r.text

    def write_file(self,filename=None,data=None):
        #Add some validations here
        try:
            fh = open(filename,'w')
        except:
            logger.error("Unexpected error:{0}".format(sys.exc_info()[0]))
        try:
            fh.write(data)
            fh.close()
        except:
            logger.error("Unexpected error:{0}".format(sys.exc_info()[0]))

    def write_json_to_file(self, filename=None, json_data=None):
        data = json.dumps(json_data,indent=4)
        self.write_file(filename,data)

    def get_amc_codes(self):
        r = self.get_url_data(self.CODES_URL)
        if r:
            soup = bs4.BeautifulSoup(r, 'html.parser')
            NavDownMFName =  soup.find_all("select",id="NavDownMFName")
            amc_codes = { e.string:e.attrs['value'] for e in NavDownMFName[0].findAll('option') if e.attrs['value'] != '' and e.string != 'All'}
        else:
            amc_codes = None
        return amc_codes

    def get_max_delta(self):
        if os.path.isfile(os.path.join(self.config['cache_path'],'lockdata.json')):
            lockdata = json.load(open(os.path.join(self.config['cache_path'],'lockdata.json')))
        else:
            lockdata = dict()

        for d in lockdata.values():
            max_delta = 0
            today = datetime.datetime.today()
            if (today - datetime.datetime.strptime(d,'%d-%b-%Y')).days > max_delta:
                max_delta = (today - datetime.datetime.strptime(d,'%d-%b-%Y')).days
        return max_delta

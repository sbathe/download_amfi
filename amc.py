from config import load_config
import datetime
from utils import utils
import bs4
import os
import time
import json
from log import MyLog
log = MyLog(loggername='get-data')
logger, rootlogger = MyLog.setup_logging(log)

class Amc:
    def __init__(self, amcname=None, amccode=None, start_date=None,end_date=None):
        self.CODES_URL = 'https://www.amfiindia.com/nav-history-download'
        self.NAV_HISTORY_URL_TEMPLATE = 'https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?tp=1&'
        self.START_DATE = '01-Apr-1990'
        self.END_DATE = datetime.datetime.strftime(datetime.datetime.today(),'%d-%b-%Y')
        if start_date == None:
            self.start_date = self.START_DATE
        else:
            self.start_date = start_date
        if end_date == None:
            self.end_date = self.END_DATE
        else:
            self.end_date = end_date
        self.name = amcname
        self.code = amccode
        self.config = load_config()

    def __str__(self):
        return """
              AMC Name: {},
              AMC Code: {},
              Start Date: {},
              End Date: {},
              Codes URL: {},
              NAV URL Template: {},
              NAV URL: {},
              Config: {}
             """.format(self.name,self.code,self.start_date,self.end_date,self.CODES_URL,
                        self.NAV_HISTORY_URL_TEMPLATE, self.NAV_HISTORY_URL, str(self.config))

    def __repr__(self):
        return f"""Amc(name={self.name},
        code={self.code},
        start_date={self.start_date},
        end_date={self.end_date},
        CODES_URL={self.CODES_URL},
        NAV_HISTORY_URL_TEMPLATE={self.NAV_HISTORY_URL_TEMPLATE},
        config={self.config}
        """

    def get_start_date(self):
        if os.path.isfile(os.path.join(self.config['cache_path'],'lockdata.json')):
            d = json.load(open(os.path.join(self.config['cache_path'],'lockdata.json')))
            try:
                s_date = datetime.datetime.strftime(datetime.datetime.strptime(d[self.name],'%d-%b-%Y') + datetime.timedelta(days=1), '%d-%b-%Y')
            except KeyError:
                s_date = self.START_DATE
        else:
            s_date = self.START_DATE
        return s_date

    def validate_date(self,start_date=None,end_date=None):
        if start_date is None:
            start_date = self.get_start_date()
        if end_date is None:
            end_date = self.END_DATE
        return (start_date,end_date)

    def get_amc_nav_data(self, start_date=None, end_date=None):
        logger.debug(f"Before validate call: Start and end date: {start_date} {end_date}")
        start_date, end_date = self.validate_date(start_date,end_date)
        logger.debug(f"Start and end date: {start_date} {end_date}")
        if isinstance(self.code, str) or isinstance(self.code, int):
            url = self.NAV_HISTORY_URL_TEMPLATE + \
                'frmdt=' + start_date + '&todt=' + end_date + \
                '&mf=' + str(self.code)
        else:
            url = None
        logger.info("Downloading data for {}".format(self.name))
        logger.debug("calling URL: {0}".format(url))
        data = utils.get_url_data(self, url)
        return data

    def write_cache_file(self, data=None):
        if not data:
            print('No Data provided, no need to write files')
            return
        lockfile = os.path.join(self.config['cache_path'],'lockdata.json')
        if os.path.exists(lockfile):
            lockdata = json.load(open(lockfile))
        else:
            lockdata = dict()
        name = '_'.join(self.name.split())
        today = datetime.datetime.strftime(datetime.datetime.strptime(self.end_date, '%d-%b-%Y'), '%s')
        filename = '_'.join([name, today]) + '.csv'
        if not os.path.exists(self.config['cache_path']):
            os.mkdir(self.config['cache_path'])
        try:
            outf = open(os.path.join(self.config['cache_path'],filename),'w+')
            outf.write(data)
            outf.close()
        except Exception as e:
            logger.error("Cannot create file: {0}".format(e))
        lockdata[self.name] = self.end_date
        try:
            outf = open(lockfile,'w')
            outf.write(json.dumps(lockdata,sort_keys=True, indent=4))
            outf.close()
        except Exception as e:
            logger.error("Data Written, cannot write lockfile: {0}".format(e))


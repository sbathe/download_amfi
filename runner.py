from amc import Amc
from amcparser import Parseamc
from data_writer import Amcwriter
from utils import utils
import datetime
import json
import os
import time

NAVALL_URL_TEMPLATE = 'http://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?frmdt=START_DATE&todt=END_DATE'

u = utils()
p = Parseamc()
writer = Amcwriter()
amc_codes = u.get_amc_codes()

# Download raw data for all AMCs
def download_data(a: Amc, delta: int) -> str:
    start_date, end_date = a.validate_date()
    today = datetime.datetime.today()
    if datetime.datetime.strptime(start_date,'%d-%b-%Y').weekday() >= 5:
            if (today - datetime.datetime.strptime(start_date,'%d-%b-%Y')).days <= 1:
                print('No need to get data, already updated locally')
                return None
    elif (today - datetime.datetime.strptime(start_date,'%d-%b-%Y')).days == 0:
                print('No need to get data, already updated locally')
                return None
    if delta > 0 and delta < 30:
        # We have a delta less than a month. We can download just one file with all NAV updates
        url = NAVALL_URL_TEMPLATE.replace('START_DATE',start_date).replace('END_DATE',end_date)
    else:
        # delta is much higher, lets get individual files
        url = a.NAV_HISTORY_URL
    return a.get_amc_nav_data(start_date=start_date, end_date=end_date, url=url)

def do_amc_loop(amc_codes, delta):
    for name, code in amc_codes.items():
        print('Downloading data for {}'.format(name))
        a = Amc(name, code)
        d = download_data(a, delta)
        if d:
            a.write_cache_file(d)
        time.sleep(5)

        # Parse and put the AMC files as csvs
        json_data = p.get_json_from_amc_csvs(a.name)
        writer.write_schemewise_data(json_data)

delta = u.get_max_delta()
do_amc_loop(amc_codes, delta)
"""
delta = u.get_max_delta()
if delta < 0 or delta > 30:
    do_amc_loop(amc_codes)
else:
    a = Amc(amcname='ALL',amccode='99999')
    d = download_data(a, delta)
    if d:
        a.write_cache_file(d)
    time.sleep(5)

    # Parse and put the AMC files as csvs
    json_data = p.get_json_from_amc_csvs(a.name)
    writer.write_schemewise_data(json_data)
"""

from amc import Amc
from amcparser import Parseamc
from data_writer import Amcwriter
from utils import utils
import datetime
import time

NAVALL_URL_TEMPLATE = """
http://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?frmdt=START_DATE&todt=END_DATE
"""

u = utils()
p = Parseamc()
writer = Amcwriter()
amc_codes = u.get_amc_codes()


# Download raw data for all AMCs
def download_data(a: Amc, delta: int) -> str:
    start_date, end_date = u.validate_date()
    a.start_date = start_date
    a.end_date = end_date
    print(f"Start and end date: {start_date} {end_date}")
    today = datetime.datetime.today()
    if datetime.datetime.strptime(start_date, '%d-%b-%Y').weekday() >= 5:
        if (today - datetime.datetime.strptime(start_date,
                                               '%d-%b-%Y')).days <= 1:
            print('No need to get data, already updated locally')
            return None
    elif (today - datetime.datetime.strptime(start_date,
                                             '%d-%b-%Y')).days == 0:
        print('No need to get data, already updated locally')
        return None
    else:
        return a.get_amc_nav_data(start_date=start_date, enddate=end_date)


def get_all_data(amc_codes=amc_codes, start_date=None, end_date=None):
    # We have a delta less than a month. We can download
    # just one file with all NAV updates
    start_date, end_date = u.validate_date()
    url = NAVALL_URL_TEMPLATE.replace('START_DATE',
                                      start_date).replace('END_DATE', end_date)
    data = utils.get_url_data(url)
    # Parse and put the AMC files as csvs
    for name, code in amc_codes.items():
        a = Amc(name, code)
        json_data = p.get_json_from_amc_csvs(a.name)
        writer.write_schemewise_data(json_data)


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
print(f"delta: {delta} days")

if delta < 30:
    get_all_data()
else:
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

import time
import json
from datetime import timedelta, datetime
from string import Template
import requests
from tinydb import TinyDB
import config

API_KEY = config.ENPHASE_API_ID
API_ID = config.ENPHASE_API_ID
USER_ID = config.ENPHASE_USER_ID
SITE_ID = config.ENPHASE_SITE_ID
DATA_BASE = config.ENPHASE_DATABASE
DATA_BASE_GEN_TABLE = config.ENPHASE_DATABASE_GEN_TABLE
DATA_BASE_CONS_TABLE = config.ENPHASE_DATABASE_CONS_TABLE
URL = config.ENPHASE_URL

def generate_epoch(mytime):
    mytime = datetime.strptime(mytime, "%Y-%m-%d").timestamp()
    mytime = int(mytime)
    return mytime

def generate_reg_time(mytime):
    mytime = datetime.fromtimestamp(mytime).strftime('%Y-%m-%d')
    return mytime

def generate_dates(sdate, edate=''):
    if not edate:
        edate = sdate
    mydates = []
    sdate = datetime.strptime(sdate, "%Y-%m-%d")
    edate = datetime.strptime(edate, "%Y-%m-%d")
    delta = edate - sdate       # as timedelta
    for i in range(delta.days + 1):
        day = sdate + timedelta(days=i)
        day = day.strftime("%Y-%m-%d")
        mydates.append(day)
    return mydates

def generate_time_difference(event_end_time):
    now = datetime.now()
    generate_reg_time(event_end_time)
    diff = event_end_time - now
    return diff

def generate_url(event_request, event_start_date="", event_end_date=""):
    try:
        event_start_date = datetime.strptime(event_start_date, "%Y-%m-%d").strftime("%Y-%m-%d")
    except ValueError:
        print("There must be a start date defined with in a YYYY-MM-DD format.")
        raise
    except:
        print("An unexpected error has occurred with the 'start date'.")
        raise
    if event_request == "generation":
        # Stats can only return at most, one day. End_at is for another time interval during the same day.
        REQUEST = "stats"
    elif event_request == "consumption":
        # consumption_stats can return at most one month worth of data.
        REQUEST = "consumption_stats"
    else:
        print("INCORRECT EVENT REQUEST")

    if event_end_date:
        event_url = Template('https://api.enphaseenergy.com/api/v2/systems/$SITE_ID/$REQUEST?key=$API_KEY&user_id=$USER_ID')
        filled_url = event_url.substitute(SITE_ID=SITE_ID, REQUEST=REQUEST, USER_ID=USER_ID, API_KEY=API_KEY)
        try:
            event_end_date = datetime.strptime(event_end_date, "%Y-%m-%d").strftime("%Y-%m-%d")
            epoch_end_date = generate_epoch(event_end_date)
        except ValueError:
            print("There must be an end date defined with in a YYYY-MM-DD format.")
            raise
        except:
            print("An unexpected error has occurred with the 'end date'.")
        if event_request == "generation":
            epoch_start_date = generate_epoch(event_start_date)
            event_url = Template('https://api.enphaseenergy.com/api/v2/systems/$SITE_ID/$REQUEST?key=$API_KEY&user_id=$USER_ID')
            filled_url = event_url.substitute(SITE_ID=SITE_ID, REQUEST=REQUEST, USER_ID=USER_ID, API_KEY=API_KEY)
            final_url = filled_url + "&start_at={}&datetime_format=iso8601".format(epoch_start_date)
            return final_url, event_start_date
        if event_request == "consumption":
            epoch_start_date = generate_epoch(event_start_date)
            final_url = filled_url + "&start_at={}&end_at={}&datetime_format=iso8601".format(epoch_start_date, epoch_end_date)
            return final_url, event_start_date
    elif event_start_date:
        event_url = Template('https://api.enphaseenergy.com/api/v2/systems/$SITE_ID/$REQUEST?key=$API_KEY&user_id=$USER_ID')
        filled_url = event_url.substitute(SITE_ID=SITE_ID, REQUEST=REQUEST, USER_ID=USER_ID, API_KEY=API_KEY)
        epoch_start_date = generate_epoch(event_start_date)
        final_url = filled_url + "&start_at={}&datetime_format=iso8601".format(epoch_start_date)
        return final_url, event_start_date
    else:
        event_url = Template('https://api.enphaseenergy.com/api/v2/systems/$SITE_ID/$REQUEST?key=$API_KEY&user_id=$USER_ID&datetime_format=iso8601')
        return event_url.substitute(SITE_ID=SITE_ID, REQUEST=REQUEST, USER_ID=USER_ID, API_KEY=API_KEY)

def append_db(rawdata, newdate=""):
    data = rawdata['intervals']
    print("Adding {} records ...".format(len(data)))
    for entry in data:
        entrydate = entry['end_at'][0:10]
        entrytime = entry['end_at'][11:19]
        energy_watt_hr = entry['enwh']
        try:
            power = entry['powr']
            GENERATION_TABLE.insert({"date":entrydate, "time": entrytime, "EnWh": energy_watt_hr, "Power": power})
        except:
            CONSUMPTION_TABLE.insert({"date":entrydate, "time": entrytime, "EnWh": energy_watt_hr})
    return

def request_data(url):
    print('Getting data')
    r = requests.get(url)
    rawdata = r.json()
    try:
        testnull = rawdata['intervals']
        print(rawdata['intervals'][0])
    except NameError:
        print("No Data available for this date.")
        raise
    except KeyError:
        THROTTLING_CHECKING = True
        while THROTTLING_CHECKING:
            check_throttling_and_rest(rawdata)
            break
    else:
        if not testnull:
            print("Successful request, however, no data available for this date.")
            raise
    return rawdata

def save_data(rawdata, mydate, event_request):
    with open(mydate + "-" + event_request + '.txt', 'w') as outfile:
        json.dump(rawdata, outfile)
    return rawdata

def check_throttling_and_rest(rawData):
    print('Checking throttling....')
    print(rawData)
    try:
        if rawData['reason']:
            print('Throttling alert, please wait until 1 minute.')
            time.sleep(60) # You get 10 API calls per minute, i.e. 1 every 6 seconds.
            THROTTLING_CHECKING = False
    except KeyError: # No Throttling
        print('No throttling needed.')
        THROTTLING_CHECKING = False

def main(event_request, event_start_date="", event_end_date=""):
    if event_request is "generation":
        dates = generate_dates(event_start_date, event_end_date)
        for newdate in dates:
            url, mydate = generate_url(event_request, newdate)
            print(url)
            rawdata = request_data(url)
            save_data(rawdata, mydate, event_request)
            append_db(rawdata, newdate=mydate)
    else:
        url, mydate = generate_url(event_request, event_start_date, event_end_date)
        print(url)
        rawdata = request_data(url)
        save_data(rawdata, mydate, event_request)
        append_db(rawdata, newdate=mydate)
    print("FINISHED")

DB = TinyDB(DATA_BASE)
GENERATION_TABLE = DB.table(DATA_BASE_GEN_TABLE)
CONSUMPTION_TABLE = DB.table(DATA_BASE_CONS_TABLE)

#main("generation", event_start_date='2020-07-01', event_end_date='2020-07-13')
main("consumption", event_start_date='2020-06-01', event_end_date='2020-07-01')

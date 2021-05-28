import argparse
import datetime
import json
import logging

import requests
from jwt import JWT
from tinydb import TinyDB

import local_credentials

log_file_name = "flume.log"
config = {}


def checkparams():
    parser = argparse.ArgumentParser(description="Utility for exporting Flume data")

    # parser.add_argument("--clientid", help="Flume client API")
    # parser.add_argument("--clientsecret", help="Flume client secret")

    # parser.add_argument(
    #    "--username", help="Flume username.  Only required to obtain initial token."
    # )
    # parser.add_argument(
    #    "--password",
    #    help="Flume client secret.  Only required to obtain initial token.",
    # )

    parser.add_argument(
        "--tokenfile",
        default="flume.token",
        help="Token details file.  This file will be written to when in --auth mode.  This file will be read from for all other modes.",
    )
    parser.add_argument(
        "--DBfile",
        default="db2.json",
        help="Append TinyDB table with records, default is db2.json",
    )
    parser.add_argument(
        "--DBtable",
        default="H2O_Usage_in_gallon",
        help="Name of table for database, default is H2O_Usage_in_gallon",
    )
    parser.add_argument(
        "--startDate",
        dest="startDate",
        type=lambda s: datetime.datetime.strptime(s, "%Y-%m-%d"),
        help="Enter start date in YYYY-MM-DD format",
    )
    parser.add_argument(
        "--endDate",
        dest="endDate",
        default=f'{datetime.datetime.now().strftime("%Y-%m-%d")}',
        type=lambda s: datetime.datetime.strptime(s, "%Y-%m-%d"),
        help="Enter end date in YYYY-MM-DD format",
    )

    parser.add_argument("--verbose", "-v", help="Add verbosity", action="store_true")
    parser.add_argument(
        "--interval",
        "-t",
        help="How frequently should Flume be contacted",
        action="store_true",
    )

    action_group = parser.add_mutually_exclusive_group()
    action_group.add_argument(
        "--auth", help="Obtain authentication token", action="store_true"
    )
    action_group.add_argument("--renew", help="Renew auth token", action="store_true")
    action_group.add_argument(
        "--details",
        help="Get important metadata about your Flume account",
        action="store_true",
    )
    action_group.add_argument(
        "--query",
        dest="query",
        help="Query water usage for last minute",
        action="store_true"
    )
    action_group.add_argument(
        "--getBulkData",
        dest="getBulkData",
        help="Query water usage over a series of day(s)",
        action="store_true",
    )

    args = parser.parse_args()

    config["clientid"] = local_credentials.FLUME_CLIENT_ID  # args.clientid
    config["clientsecret"] = local_credentials.FLUME_CLIENT_SECRET  # args.clientsecret
    config["username"] = local_credentials.FLUME_USERNAME  # args.username
    config["password"] = local_credentials.FLUME_PASSWORD  # args.password
    config["tokenfile"] = args.tokenfile
    config["appendDB"] = args.DBfile
    config["table"] = args.DBtable
    config["startDate"] = args.startDate
    config["endDate"] = args.endDate
    config["verbose"] = args.verbose
    config["interval"] = args.interval

    if args.auth:
        config["mode"] = "auth"
    else:
        loadCredentials(config)

    if args.details:
        config["mode"] = "details"
    if args.query:
        config["mode"] = "query"
    if args.renew:
        config["mode"] = "renew"
    if args.getBulkData:
        config["mode"] = "getBulkData"

    if (config["mode"] == "getBulkData") and (config["startDate"] is None):
        args = argparse.ArgumentParser()
        message = "--getBulkData option requires --startDate (Optional: --endDate)."
        logging.debug(message)
        raise args.error(message=message)

    return config


def setup_logger(name, log_file, level=logging.INFO):
    """Function setup as many loggers as you want"""
    handler = logging.FileHandler(log_file)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)
    return logger


def obtainCredentials(config):
    logger.info("Getting auth token")
    if config["verbose"]:
        logging.info("Getting auth token")

    if (
        config["clientid"]
        and config["clientsecret"]
        and config["username"]
        and config["password"]
    ):
        if config["verbose"]:
            logging.info("All required parameters passed for auth token")
        url = "https://api.flumetech.com/oauth/token"
        payload = (
            '{"grant_type":"password","client_id":"'
            + config["clientid"]
            + '","client_secret":"'
            + config["clientsecret"]
            + '","username":"'
            + config["username"]
            + '","password":"'
            + config["password"]
            + '"}'
        )
        headers = {"content-type": "application/json"}

        resp = requests.request("POST", url, data=payload, headers=headers)
        logging.info(f"Response from server: {resp.text}")
        dataJSON = json.loads(resp.text)

        if dataJSON["http_code"] == 200:
            logging.info("Got 200 response from auth token request.")
            config["access_token"] = dataJSON["data"][0]["access_token"]
            token = JWT()
            decoded_token = token.decode(
                config["access_token"], do_verify=False, algorithms="HS256"
            )
            config["user_id"] = decoded_token["user_id"]
            config["refresh_token"] = dataJSON["data"][0]["refresh_token"]

            if config["tokenfile"]:
                outline = {}
                outline["access_token"] = config["access_token"]
                outline["refresh_token"] = config["refresh_token"]
                logging.info(
                    "Saving access and refresh token to : " + config["tokenfile"]
                )
                logging.debug(outline)
                f = open(config["tokenfile"], "w")
                f.write(json.dumps(outline))
                f.close()
        else:
            logging.CRITICAL("ERROR: Failed to obtain credentials")


def renewCredentials(config):
    url = "https://api.flumetech.com/oauth/token"
    payload = (
        '{"grant_type":"refresh_token", "refresh_token":"'
        + config["refresh_token"]
        + '", "client_id":"'
        + config["clientid"]
        + '", "client_secret":"'
        + config["clientsecret"]
        + '"}'
    )
    resp = requests.request("POST", url, data=payload, headers=headers)
    dataJSON = json.loads(resp.text)
    logging.debug(f"Credentials data: {dataJSON}")


def loadCredentials(config):
    if not config["tokenfile"]:
        quit("You have to provide a token file.")
        logging.CRITICAL("You have to provide a token file.")
    else:
        logging.debug(f"Reading token info from: <{config['tokenfile']}>")
        with open(config["tokenfile"], "r") as f:
            token = json.load(f)
        config["access_token"] = token["access_token"]
        config["refresh_token"] = token["refresh_token"]
        token = JWT()
        decoded_token = token.decode(
            config["access_token"], do_verify=False, algorithms="HS256"
        )
        config["user_id"] = decoded_token["user_id"]


def buildRequestHeader():
    header = {"Authorization": "Bearer " + config["access_token"]}
    return header


def testAuthorizationToken():
    resp = requests.request(
        "GET", "https://api.flumetech.com/users/11382", headers=buildRequestHeader()
    )
    # print(resp.text);
    dataJSON = json.loads(resp.text)
    return dataJSON["http_code"] == 200


def previousminute():
    return (datetime.datetime.now() - datetime.timedelta(minutes=1)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )


def currentminute():
    # return (datetime.datetime.now() - datetime.timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M:%S');
    return (datetime.datetime.now()).strftime("%Y-%m-%d %H:%M:%S")


def calculateTimes(startTime, endTime, interval):
    if interval == "1":
        max_requests = 1200
    diff = endTime - startTime
    if diff >= datetime.timedelta(hours=20):
        endTime = startTime + datetime.timedelta(hours=19, minutes=59)
        endTime = endTime.strftime("%Y-%m-%d %H:%M:%S")
        return endTime
    else:
        return endTime


def getDevices(config):
    logging.info("Retrieving latest device(s) info")
    resp = requests.request(
        "GET",
        "https://api.flumetech.com/users/" + str(config["user_id"]) + "/devices",
        headers=buildRequestHeader(),
    )
    dataJSON = json.loads(resp.text)
    if dataJSON["http_code"] == 401:
        obtainCredentials(config)
    logging.info("Executed device search")
    if dataJSON["http_code"] == 200:
        logging.debug(f'Latest complete device info:\n\t{dataJSON["data"]}')
        for bridge in dataJSON["data"]:
            if bridge["type"] == 2:
                config["device_id"] = bridge["id"]


def getWaterFlowLastMinute():
    payload = (
        '{"queries":[{"request_id":"perminute","bucket":"MIN","since_datetime":"'
        + previousminute()
        + '","until_datetime":"'
        + currentminute()
        + '","group_multiplier":"1","operation":"SUM","sort_direction":"ASC","units":"GALLONS"}]}'
    )
    headers = buildRequestHeader()
    headers["content-type"] = "application/json"
    resp = requests.request(
        "POST",
        "https://api.flumetech.com/users/"
        + str(config["user_id"])
        + "/devices/"
        + str(config["device_id"])
        + "/query",
        data=payload,
        headers=headers,
    )
    data = json.loads(resp.text)
    if data["http_code"] == 200:
        return data["data"][0]["perminute"][0]["value"]
    else:
        return None


def append_db(rawdata):
    DB = TinyDB(config["appendDB"])
    WATER_USAGE_TABLE = DB.table(config["table"])
    for ampm in rawdata:
        logging.info(
            f"Adding {len(ampm)} records starting with {ampm[0]['datetime']}..."
        )
        for entry in ampm:
            entrydate = entry["datetime"][0:10]
            entrytime = entry["datetime"][11:19]
            entryusage = entry["value"]
            WATER_USAGE_TABLE.insert(
                {"date": entrydate, "time": entrytime, "gallons": entryusage}
            )
    return


def getBulkData():
    startDate = config["startDate"]
    endDate = config["endDate"]
    delta = endDate - startDate
    data = []
    logging.info(f"Bulk data requested:\n\tGetting info from {startDate} to {endDate}.")
    for i in range(delta.days + 1):
        day = startDate + datetime.timedelta(days=i)
        eventStartDate1 = day.strftime("%Y-%m-%d 00:00:00")  # startTime
        eventEndDate1 = day.strftime(
            "%Y-%m-%d 11:59:00"
        )  # endTime, 19:59:00 is the latest possible time.
        eventStartDate2 = day.strftime("%Y-%m-%d 12:00:00")  # startTime
        eventEndDate2 = day.strftime("%Y-%m-%d 24:00:00")  # endTime
        payload1 = (
            '{"queries":[{"request_id":"perminute","bucket":"MIN","since_datetime":"'
            + eventStartDate1
            + '","until_datetime":"'
            + eventEndDate1
            + '","group_multiplier":"1","sort_direction":"ASC","units":"GALLONS"}]}'
        )
        payload2 = (
            '{"queries":[{"request_id":"perminute","bucket":"MIN","since_datetime":"'
            + eventStartDate2
            + '","until_datetime":"'
            + eventEndDate2
            + '","group_multiplier":"1","sort_direction":"ASC","units":"GALLONS"}]}'
        )
        headers = buildRequestHeader()
        headers["content-type"] = "application/json"
        resp1 = requests.request(
            "POST",
            "https://api.flumetech.com/users/"
            + str(config["user_id"])
            + "/devices/"
            + str(config["device_id"])
            + "/query",
            data=payload1,
            headers=headers,
        )
        resp2 = requests.request(
            "POST",
            "https://api.flumetech.com/users/"
            + str(config["user_id"])
            + "/devices/"
            + str(config["device_id"])
            + "/query",
            data=payload2,
            headers=headers,
        )
        data1 = json.loads(resp1.text)
        data2 = json.loads(resp2.text)
        if (data1["http_code"] == 200) and (data2["http_code"] == 200):
            data.append(data1["data"][0]["perminute"])
            data.append(data2["data"][0]["perminute"])
        elif (data1["http_code"] == 429) and (data2["http_code"] == 429):
            logging.debug(f'Data1 detailed: \ndata1["detailed"]\n\n')
            break
        elif data1["http_code"] == 429:
            logging.debug(f'Data1 detailed: \ndata1["detailed"]\n\n')
            break
        else:
            data.append(data1["data"][0]["perminute"])
            return data
    return data


def transmitFlow(flowValue):
    if config["appendDB"]:
        append_db(flowValue)


def main():
    global logger
    global config
    logger = setup_logger("", log_file_name, level=logging.DEBUG)

    config = checkparams()

    if config["mode"] == "auth":
        obtainCredentials(config)

    if config["mode"] == "renew":
        loadCredentials(config)
        renewCredentials(config)

    if config["mode"] == "details":
        loadCredentials(config)
        getDevices(config)
        print("-------------------------------------------")
        print("Access Token: " + config["access_token"])
        print("Refresh Token: " + config["refresh_token"])
        print("User ID: " + str(config["user_id"]))

    if config["mode"] == "query":
        loadCredentials(config)
        getDevices(config)
        transmitFlow(getWaterFlowLastMinute())

    if config["mode"] == "getBulkData":
        loadCredentials(config)
        getDevices(config)
        transmitFlow(getBulkData())

    if config["mode"] == "lastMinute":
        loadCredentials(config)
        getDevices(config)
        getWaterFlowLastMinute()


main()

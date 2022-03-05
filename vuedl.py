#!/usr/bin/env python3

from configparser import ConfigParser
from datetime import datetime
from datetime import timedelta
from datetime import timezone
import logging
import requests
import sys
import time

logging.basicConfig(filename="/var/log/vuedl.log", level=logging.DEBUG, format="%(asctime)s: [%(name)s] [%(levelname)s] %(message)s")

def logging_hook(res: requests.Response, *args, **kwargs):
    req = res.request

    logging.debug("================================================================================")

    logging.debug(f"{req.method:6} {req.url}")

    if req.headers is not None:
        for header in req.headers:
            logging.debug(f"{header}: {req.headers[header]}")

    if req.body is not None:
        logging.debug(req.body)

    logging.debug("--------------------------------------------------------------------------------")

    logging.debug(f"{res.status_code} {res.reason}")

    if res.headers is not None:
        for header in res.headers:
            logging.debug(f"{header}: {res.headers[header]}")

    if res.text is not None:
        logging.debug(res.text)

    logging.debug("================================================================================")

api = requests.Session()
api.hooks["response"] = [logging_hook]

def get_token(username: str, password: str, auth_url: str, client_id: str):
    response = api.post(
        auth_url,
        headers={
            "Content-Type": "application/x-amz-json-1.1",
            "X-Amz-Target": "AWSCognitoIdentityProviderService.InitiateAuth"
        },
        json={
            "AuthParameters": {
            "USERNAME": username,
            "PASSWORD": password
            },
            "AuthFlow": "USER_PASSWORD_AUTH",
            "ClientId": client_id
        }
    )
    response.raise_for_status()
    json = response.json()
    token = json["AuthenticationResult"]["IdToken"]
    token_expiration = datetime.now(timezone.utc) + timedelta(seconds = json["AuthenticationResult"]["ExpiresIn"])
    return token, token_expiration

def get_customer_gid(email: str, token: str, api_url: str) -> int:
    response = api.get(
        api_url + "/customers" + "?email=" + email,
        headers = {"authtoken": token}
    )
    response.raise_for_status()
    json = response.json()
    customer_gid = str(json["customerGid"])
    return int(customer_gid)

def get_devices(customer_gid: int, token: str, api_url: str):
    response = api.get(
        api_url + "/customers/devices",
        headers = {"authtoken": token}
    )
    response.raise_for_status()
    json = response.json()
    devices = []
    for device in json["devices"]:
        for channel in device["channels"]:
            devices.append({"device_gid": int(channel["deviceGid"]), "channel": channel["channelNum"]})
        for sub_device in device["devices"]:
            for sub_device_channel in sub_device["channels"]:
                devices.append({"device_gid": int(sub_device_channel["deviceGid"]), "channel": sub_device_channel["channelNum"]})
    return devices

def get_device_usage_data(device_gid: int, channel: str, start: datetime, end: datetime, scale: str, token: str, api_url: str) -> str:
    response = api.get(
        api_url + "/AppAPI?apiMethod=getChartUsage&deviceGid=" + str(device_gid)
                + "&channel=" + channel
                + "&start=" + start.isoformat().replace("+00:00", "Z")
                + "&end=" + end.isoformat().replace("+00:00", "Z")
                + "&scale=" + scale
                + "&energyUnit=KilowattHours",
        headers = {"authtoken": token}
    )
    response.raise_for_status()
    return response.text

def truncate_seconds(t: datetime) -> datetime:
    return datetime(year=t.year, month=t.month, day=t.day, hour=t.hour, minute=t.minute, tzinfo=t.tzinfo)

def save_config(filename: str, config: ConfigParser):
    with open(filename, "w") as f:
        config.write(f)

def main():
    verbose = False
    if "-v" in sys.argv:
        verbose = True

    config_file = "/etc/vuedl.conf"
    config = ConfigParser()
    config.read(config_file)

    # Request data up until 5 minutes ago
    end = datetime.now(timezone.utc)
    end = truncate_seconds(end)
    end = end - timedelta(minutes=5, seconds=1)

    # Request data starting from the last run, or 60 minutes ago if this is our first run
    start = datetime.fromisoformat(config.get("runtime", "last_run")) + timedelta(seconds=1) if config.has_option("runtime", "last_run") else end - timedelta(minutes=60)

    delta = end - start
    if (delta.days == 0 and delta.seconds < 60):
        sys.exit("Less than one minute between start and end, abort.")

    if verbose: print("Obtaining data between", start, "and", end)

    username = config.get("config", "username")
    password = config.get("config", "password")
    api_url = config.get("config", "api_url")
    auth_url = config.get("config", "auth_url")
    client_id = config.get("config", "client_id")
    data_folder = config.get("config", "data_folder")

    token = config.get("runtime", "token")
    token_expiration_str = config.get("runtime", "token_expiration")
    token_expiration = datetime.fromisoformat(token_expiration_str) if len(token_expiration_str) else datetime.utcnow()

    if (datetime.now(timezone.utc) + timedelta(seconds=30) > token_expiration):
        if verbose: print("Need to obtain token")
        token, token_expiration = get_token(username, password, auth_url, client_id)
        config.set("runtime", "token", token)
        config.set("runtime", "token_expiration", token_expiration.isoformat())
        save_config(config_file, config)
        if verbose: print("Token obtained, expires", token_expiration)

    time.sleep(1)
    customer_gid = config.getint("runtime", "customer_gid") if config.has_option("runtime", "customer_gid") else 0
    if (customer_gid == 0):
        if verbose: print("Need to obtain customer GID")
        customer_gid = get_customer_gid(username, token, api_url)
        config.set("runtime", "customer_gid", str(customer_gid))
        save_config(config_file, config)
        if verbose: print("Customer GID obtained, GID =", customer_gid)

    time.sleep(1)
    devices = get_devices(customer_gid, token, api_url)
    for device in devices:
        device_gid = device["device_gid"]
        channel = device["channel"]
        if verbose: print("Obtain usage data for device", device_gid, "channel", channel)
        for i in range(3):
            for scale in ["1MIN"]:
                try:
                    time.sleep(1)
                    usage_data = get_device_usage_data(device_gid, channel, start, end, scale, token, api_url)
                    if len(usage_data) > 0:
                        filename = data_folder + "vue_" + str(device_gid) + "_" + channel + "_" + start.isoformat().replace("+00:00", "Z") + "-" + end.isoformat().replace("+00:00", "Z") + "_" + scale + ".json"
                        with open(filename, "w") as f:
                            f.write(usage_data)
                        if verbose: print(usage_data)
                        break
                except Exception as e:
                    print(e)
                    if i == 2:
                        raise
                    print("wait 30 seconds and try again")
                    time.sleep(30)

    config.set("runtime", "last_run", end.isoformat())
    save_config(config_file, config)
    if verbose: print("Done!")

if __name__ == "__main__":
  main()


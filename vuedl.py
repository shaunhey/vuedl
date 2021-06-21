#!/usr/bin/env python3

from configparser import ConfigParser
from datetime import datetime
from datetime import timedelta
from datetime import timezone
import requests

def get_token(username: str, password: str, auth_url: str, client_id: str) -> tuple[str, datetime]:
    response = requests.post(
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
    print("Obtained token, expires at", token_expiration)
    return token, token_expiration

def get_customer_gid(email: str, token: str, api_url: str) -> int:
    response = requests.get(
        api_url + "/customers" + "?email=" + email,
        headers = {"authtoken": token}
    )
    response.raise_for_status()
    json = response.json()
    customer_gid = str(json["customerGid"])
    return int(customer_gid)

def get_device_gids(customer_gid: int, token: str, api_url: str) -> list[int]:
    response = requests.get(
        api_url + "/customers/" + str(customer_gid) + "/devices",
        headers = {"authtoken": token}
    )
    response.raise_for_status()
    json = response.json()
    device_gids = []
    for device in json["devices"]:
        device_gids.append(int(device["deviceGid"]))
    return device_gids

def get_device_usage_data(device_gid: int, start: datetime, end: datetime, token: str, api_url: str) -> str:
    response = requests.get(
        api_url + "/AppAPI?apiMethod=getChartUsage&deviceGid=" + str(device_gid)
                + "&channel=1,2,3"
                + "&start=" + start.isoformat().replace("+00:00", "Z")
                + "&end=" + end.isoformat().replace("+00:00", "Z")
                + "&scale=1MIN&energyUnit=KilowattHours",
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
    config_file = "/etc/vuedl.conf"
    config = ConfigParser()
    config.read(config_file)

    start = datetime.fromisoformat(config.get("runtime", "last_run")) if config.has_option("runtime", "last_run") else datetime.now(timezone.utc) - timedelta(minutes=61)
    start = truncate_seconds(start)

    end = datetime.now(timezone.utc) - timedelta(minutes=1)
    end = truncate_seconds(end)

    delta = end - start
    if (delta.days == 0 and delta.seconds < 60):
        print("Less than one minute between start and end, abort.")
        return

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
        print("Token expires in less than 30 seconds, need to get a new one...")
        token, token_expiration = get_token(username, password, auth_url, client_id)
        config.set("runtime", "token", token)
        config.set("runtime", "token_expiration", token_expiration.isoformat())
        save_config(config_file, config)
    else:
        print("Existing token is still valid, expires", token_expiration)

    customer_gid = config.getint("runtime", "customer_gid") if config.has_option("runtime", "customer_gid") else 0
    if (customer_gid == 0):
        print("Need to obtain customer GID...")
        customer_gid = get_customer_gid(username, token, api_url)
        config.set("runtime", "customer_gid", str(customer_gid))
        save_config(config_file, config)

    print("Customer GID:", customer_gid)

    device_gids = get_device_gids(customer_gid, token, api_url)
    print("Device GIDs:", device_gids)

    print("Obtaining usage data between", start, "and", end)

    for device_gid in device_gids:
        print("Obtaining usage data for device", device_gid)
        usage_data = get_device_usage_data(device_gid, start, end, token, api_url)
        print(usage_data)
        filename = data_folder + "vue_" + str(device_gid) + "_" + start.isoformat().replace("+00:00", "Z") + "-" + end.isoformat().replace("+00:00", "Z") + ".json"
        with open(filename, "w") as f:
            f.write(usage_data)

    config.set("runtime", "last_run", end.isoformat())
    save_config(config_file, config)

if __name__ == "__main__":
  main()

#!/usr/bin/env python3

from configparser import ConfigParser
from datetime import datetime
from datetime import timedelta
from datetime import timezone
import glob
from influxdb_client import InfluxDBClient;
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client import Point
import json
import os
import sys
import time

def get_device_gid_from_filename(filename: str) -> str:
    return filename.split("_")[1]

def get_channel_from_filename(filename: str) -> str:
    return filename.split("_")[2]

def get_scale_from_filename(filename: str) -> str:
    return filename.split("_")[4].split(".")[0]

def main():
    verbose = False
    if "-v" in sys.argv:
        verbose = True

    config_file = "/etc/vuedl.conf"
    config = ConfigParser()
    config.read(config_file)

    data_folder = config.get("config", "data_folder")

    influxdb_url = config.get("config", "influxdb_url")
    influxdb_token = config.get("config", "influxdb_token")
    influxdb_org = config.get("config", "influxdb_org")
    influxdb_bucket = config.get("config", "influxdb_bucket")

    client = InfluxDBClient(url=influxdb_url, token=influxdb_token, org=influxdb_org, timeout=60000)
    write_api = client.write_api(write_options=SYNCHRONOUS)

    for filename in glob.glob(os.path.join(data_folder, "*.json")):
        device_gid = get_device_gid_from_filename(filename)
        channel = get_channel_from_filename(filename)
        scale = get_scale_from_filename(filename)

        with open(filename) as file:
            if verbose: print("Processing", filename)
            data = json.load(file)

            t = datetime.fromisoformat(str(data["firstUsageInstant"]).replace(":00Z", ":00+00:00"))

            archive_folder = os.path.join(data_folder, "archive", t.strftime("%Y-%m-%d"))
            os.makedirs(archive_folder, exist_ok = True)

            last_usage = 0.0
            for usage in data["usageList"]:

                if usage is not None:
                    point = Point("kWh_" + scale).tag("device_gid", device_gid).tag("channel", channel).field("usage", usage).time(t)
                    if verbose: print(t, scale, device_gid, channel, usage)
                    for i in range(3):
                        try:
                            write_api.write(influxdb_bucket, influxdb_org, point)
                            break
                        except Exception as e:
                            print(e)
                            print(t, scale, device_gid, channel, usage)
                            print("Wait 30 seconds and try again")
                            time.sleep(30)
                            if i == 2:
                              raise
                    last_usage = usage

                if (scale == "1MIN"):
                    t = t + timedelta(minutes=1)
                elif (scale == "1S"):
                    t = t + timedelta(seconds=1)

        new_path = os.path.join(archive_folder, os.path.basename(filename))
        if verbose: print("Moving", filename, "to", new_path)
        os.rename(filename, new_path)

if __name__ == "__main__":
  main()

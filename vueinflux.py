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

    client = InfluxDBClient(url=influxdb_url, token=influxdb_token, org=influxdb_org)
    write_api = client.write_api(write_options=SYNCHRONOUS)

    for filename in glob.glob(os.path.join(data_folder, "*.json")):
        device_gid = get_device_gid_from_filename(filename)
        channel = get_channel_from_filename(filename)
        scale = get_scale_from_filename(filename)
        
        with open(filename) as file:
            if verbose: print("Processing", filename)
            data = json.load(file)
            
            time = datetime.fromisoformat(str(data["firstUsageInstant"]).replace(":00Z", ":00+00:00"))

            archive_folder = os.path.join(data_folder, "archive", time.strftime("%Y-%m-%d"))
            os.makedirs(archive_folder, exist_ok = True)
            
            last_usage = 0.0
            for usage in data["usageList"]:
                
                if usage is not None and usage != last_usage:
                    point = Point("kWh_" + scale).tag("device_gid", device_gid).tag("channel", channel).field("usage", usage).time(time)
                    if verbose: print(time, scale, device_gid, channel, usage)
                    write_api.write(influxdb_bucket, influxdb_org, point)
                    last_usage = usage

                if (scale == "1MIN"):
                    time = time + timedelta(minutes=1)
                elif (scale == "1S"):
                    time = time + timedelta(seconds=1)

        new_path = os.path.join(archive_folder, os.path.basename(filename))
        if verbose: print("Moving", filename, "to", new_path)
        os.rename(filename, new_path)

if __name__ == "__main__":
  main()
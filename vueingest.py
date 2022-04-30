#!/usr/bin/env python3

from datetime import datetime
from datetime import timedelta
import glob
import json
import os
import sqlite3

def get_device_name(filename: str) -> str:
  parts = filename.split('_')
  return parts[1] + '_' + parts[2]

def increment_timestamp(timestamp: str) -> str:
  t = datetime.fromisoformat(timestamp)
  t = t + timedelta(minutes=1)
  return t.isoformat()

def main():
  db = sqlite3.connect("./vue.db")
  cur = db.cursor()

  cur.execute(
    "create table if not exists readings ("
    "timestamp text not null,"
    "device text not null,"
    "value real not null,"
    "primary key(timestamp, device)"
    ");")

  for filename in glob.glob(os.path.join("/home/shaun/Documents/vue_data/*/*/", "*1MIN.json")):
    #print(f"Processing {filename}")
    device_name = get_device_name(os.path.basename(filename))

    with open(filename) as file:
      data = json.load(file)
      timestamp = data["firstUsageInstant"].replace(":00Z", ":00+00:00")
      for value in data["usageList"]:
        if value is not None:
          #print(f"{device_name} {timestamp} {value}")
          cur.execute("insert into readings (timestamp, device, value) values (?, ?, ?) on conflict do nothing;", (timestamp, device_name, value))
        timestamp = increment_timestamp(timestamp)
  db.commit()


if __name__ == "__main__":
  main()

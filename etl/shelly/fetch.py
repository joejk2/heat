"""
Fetch data from Shelly Cloud
----------------------------

Setup:
    mkdir -p <PATH>/data/archive
    cp shelly_conf_template.py shelly_conf.py  # and configure

Usage:
    python shelly_fetcher.py shelly_conf.py
"""
import datetime
import glob
import os
import shutil
import sys
import time

import requests
from heat.common.utils import from_yaml


def fetch_data(url, token, id):
    response = requests.get("{}?id={}&auth_key={}".format(url, id, token)).json()
    device_status = response["data"]["device_status"]
    return dict(
        id=id,
        update_time=device_status["_updated"],
        tmp=device_status["tmp"]["value"],
        hum=device_status["hum"]["value"],
        read_time=datetime.datetime.now().replace(microsecond=0).isoformat(),
    )


def write_data(data_dir, d):
    with open(
        "{}/log.csv".format(data_dir),
        "a",
    ) as f:
        f.write(
            "{}, {}, {}, {}, {}\n".format(
                d["id"], d["update_time"], d["tmp"], d["hum"], d["read_time"]
            )
        )


def archive_data(data_dir, archive_period_hours):
    creation_times = [
        os.path.getctime(f) for f in glob.glob("{}/archive/*log.csv".format(data_dir))
    ]
    if (
        len(creation_times) == 0
        or time.time() - max(creation_times) >= archive_period_hours * 3600
    ):
        shutil.copy2(
            "{}/log.csv".format(data_dir),
            "{}/archive/{}-log.csv".format(
                data_dir,
                datetime.datetime.now().replace(microsecond=0).isoformat(),
            ),
        )


def main(conf_path):
    conf = from_yaml(conf_path)

    for home in conf["shelly"]:
        for id in home["device_ids"]:
            data = fetch_data(home["url"], home["token"], id)
            write_data(conf["local"]["data_dir"], data)

    archive_data(conf["local"]["data_dir"], conf["local"]["archive_period_hours"])


if __name__ == "__main__":
    main(sys.argv[1])

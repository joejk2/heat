"""
Fetch data from GlowMarkt API 
"""

import sys

import pandas as pd
import requests


GLOWMARKT_API_URL = "https://api.glowmarkt.com/api/v0-1"
APPLICATION_ID = "b0f1b774-a586-4f72-9edd-27ead8aa7a8d"
HEADERS = {
    "Content-Type": "application/json",
    "applicationid": APPLICATION_ID,
}


def get_token(username, password):
    url = f"{GLOWMARKT_API_URL}/auth"
    data = {
        "username": username,
        "password": password,
    }
    response = requests.post(url, headers=HEADERS, json=data)
    return response.json()["token"]


def get_virtual_identify(token):
    url = f"{GLOWMARKT_API_URL}/virtualentity"
    response = requests.get(url, headers=dict(HEADERS, token=token))
    return response.json()[0]["veId"]


def get_resources(veId, token):
    url = f"{GLOWMARKT_API_URL}/virtualentity/{veId}/resources?=null"
    response = requests.get(url, headers=dict(HEADERS, token=token))
    return {
        r["classifier"]: r["resourceId"]
        for r in response.json()["resources"]
        if r["classifier"] in ["gas.consumption", "electricity.consumption"]
    }


def get_data_chunk(resourceId, token, start, end):
    url = (
        f"{GLOWMARKT_API_URL}/resource/{resourceId}/readings"
        f"?from={start}&to={end}&period=PT30M&function=sum&offset=0"
    )
    response = requests.get(url, headers=dict(HEADERS, token=token))
    data = pd.DataFrame(response.json()["data"], columns=["timestamp", "kWh"])
    data["timestamp"] = pd.to_datetime(data["timestamp"], unit="s")
    return data.set_index("timestamp")


def get_data(resourceId, token, start, end):
    def to_str(x):
        return x.strftime("%Y-%m-%dT%H:%M:%S")

    def to_dt(date_str):
        return pd.to_datetime(date_str)

    chunks = [
        (s, min(to_dt(end), s + pd.Timedelta("10 days")))
        for s in pd.date_range(to_dt(start), to_dt(end), freq="239.5H")
    ]

    return pd.concat(
        [get_data_chunk(resourceId, token, to_str(s), to_str(e)) for s, e in chunks]
    )


def main(username, password, start, end, output_path):
    token = get_token(username, password)
    veId = get_virtual_identify(token)
    resources = get_resources(veId, token)
    for resource, resourceId in resources.items():
        data = get_data(resourceId, token, start, end)
        data.to_csv(f"{output_path}/{username}-{resource}.csv")
    return data


if __name__ == "__main__":
    main(
        username=sys.argv[1],
        password=sys.argv[2],
        start=sys.argv[3],
        end=sys.argv[4],
        output_path=sys.argv[5],
    )

from typing import Any, Dict

import pandas as pd


def load_data(shelly_log):
    data = []
    with open(shelly_log) as file:
        for line in file:
            values = line.strip().split(",")
            if len(values) == 5:
                values.append("nan")  # battery
            data.append(values)

    df = pd.DataFrame(
        data,
        columns=[
            "device_id",
            "measured_at",
            "temperature",
            "humidity",
            "logged_at",
            "battery",
        ],
    )

    dtypes = dict(
        device_id=str,
        measured_at="datetime64[ns]",
        temperature=float,
        humidity=float,
        logged_at="datetime64[ns]",
        battery=float,
    )
    df["measured_at"] = pd.to_datetime(df["measured_at"])
    df["logged_at"] = pd.to_datetime(df["logged_at"])
    df = df.astype(dtypes)

    return df


def pivot_on_time(d):
    # When generalised, will be a pivot on `time` and `home_id`
    d["logged_at_rounded"] = d["logged_at"].dt.floor("20T")
    d_internal = d[d["device_id"] == "701878"]
    d_external = d[d["device_id"] == "caaeb0"]
    d_wide = pd.merge(
        d_internal,
        d_external,
        on="logged_at_rounded",
        how="inner",
        suffixes=["_internal", "_external"],
    )
    d_wide["temperature_difference"] = (
        d_wide["temperature_internal"] - d_wide["temperature_external"]
    )
    return d_wide


def group_by_time(d: pd.DataFrame) -> Dict[str, Any]:
    return d.assign(logged_at_rounded=d["logged_at"].dt.floor("20T")).groupby(
        "logged_at_rounded"
    )

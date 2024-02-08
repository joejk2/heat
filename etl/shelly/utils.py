from typing import Any, Dict

import pandas as pd


def load_data(shelly_log):
    return pd.read_csv(
        shelly_log,
        names=["device_id", "measured_at", "temperature", "humidity", "logged_at"],
        dtype=dict(
            device_id="str",
            measured_at="str",
            temperature=float,
            humidity=float,
            logged_at="str",
        ),
        parse_dates=["measured_at", "logged_at"],
    )


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

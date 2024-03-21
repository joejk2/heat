from typing import Any, Dict

import pandas as pd


def load_data(shelly_log, env_ids):
    data = []
    with open(shelly_log) as file:
        for line in file:
            values = line.strip().split(",")
            if values[0] not in env_ids["external"] + env_ids["internal"]:
                continue
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


def pivot_on_time(d, env_ids):
    # When generalised, will be a pivot on `time` and `home_id`
    d["logged_at_rounded"] = d["logged_at"].dt.floor("20T")

    def column_mappings(env, id):
        return {
            "temperature": f"temperature_{env}_{id}",
            "humidity": f"humidity_{env}_{id}",
            "battery": f"battery_{env}_{id}",
        }

    external_id = env_ids["external"][0]
    external_col_map = column_mappings("ext", env_ids["external"][0])
    d_wide = d[d["device_id"] == external_id].copy()[
        [
            "device_id",
            "measured_at",
            "logged_at",
            "logged_at_rounded",
            "battery",
            "temperature",
            "humidity",
        ]
    ]
    d_wide.rename(
        columns=external_col_map,
        inplace=True,
    )

    temperature_int_columns = []
    for internal_id in env_ids["internal"]:
        internal_col_map = column_mappings("int", internal_id)
        temperature_int_columns.append(internal_col_map["temperature"])

        d_internal = d[d["device_id"] == internal_id][
            ["logged_at_rounded", "battery", "temperature", "humidity"]
        ].copy()
        d_internal.rename(
            columns=internal_col_map,
            inplace=True,
        )

        d_wide = pd.merge(d_wide, d_internal, on="logged_at_rounded", how="outer")

    d_wide["temperature_int_avg"] = d_wide[temperature_int_columns].mean(axis=1)
    d_wide["temperature_diff_avg"] = (
        d_wide["temperature_int_avg"] - d_wide[external_col_map["temperature"]]
    )
    return d_wide


def group_by_time(d: pd.DataFrame) -> Dict[str, Any]:
    return d.assign(logged_at_rounded=d["logged_at"].dt.floor("20T")).groupby(
        "logged_at_rounded"
    )

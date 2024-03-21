import sys

import pandas as pd
import plotly.graph_objs
import plotly.subplots
from heat.common.utils import from_yaml
from heat.etl.shelly.utils import load_data, pivot_on_time


def load_gas_data(meter_log):
    # TODO: move function which shouldn't really be in etl/shelly
    return pd.read_csv(
        meter_log,
        names=["measured_at", "reading"],
        dtype=dict(
            measured_at=str,
            reading=float,
        ),
        parse_dates=["measured_at"],
    )


def add_degree_time_columns(d):
    d_ext = d.copy(deep=True)
    d_ext["cum_degree_days"] = (d_ext["temperature_diff_avg"]).cumsum() / (
        3 * 24  # logged every 20 minutes
    )
    d_ext["avg_degree_days"] = (
        d_ext["cum_degree_days"].iloc[-1]
        * (d_ext["logged_at_rounded"] - min(d_ext["logged_at_rounded"]))
        / (max(d_ext["logged_at_rounded"]) - min(d_ext["logged_at_rounded"]))
    )
    return d_ext


def plot(d_shelly, d_gas):
    f = plotly.subplots.make_subplots(
        rows=3,
        cols=1,
        row_heights=[0.5, 0.25, 0.25],
        subplot_titles=[
            "",
            "Avg = {:.1f} °C".format(d_shelly["temperature_diff_avg"].mean()),
            "",
        ],
        shared_xaxes=True,
        specs=[[{"secondary_y": True}]] * 3,
    )
    for col in d_shelly.filter(regex=r"^temperature_int_"):
        if col == "temperature_int_avg":
            continue
        f.add_trace(
            plotly.graph_objs.Scatter(
                x=d_shelly["logged_at_rounded"],
                y=d_shelly[col],
                name=col.replace("temperature_", ""),
            ),
            row=1,
            col=1,
        )
    for col in d_shelly.filter(regex=r"^temperature_ext_"):
        f.add_trace(
            plotly.graph_objs.Scatter(
                x=d_shelly["logged_at_rounded"],
                y=d_shelly[col],
                name=col.replace("temperature_", ""),
            ),
            row=1,
            col=1,
        )
    f.add_trace(
        plotly.graph_objs.Scatter(
            x=d_shelly["logged_at_rounded"],
            y=d_shelly["temperature_diff_avg"],
            name="difference",
        ),
        row=2,
        col=1,
    )
    f.add_trace(
        plotly.graph_objs.Scatter(
            x=d_shelly["logged_at_rounded"],
            y=[d_shelly["temperature_diff_avg"].mean()] * len(d_shelly),
            name="difference",
            line=dict(dash="dot"),
        ),
        row=2,
        col=1,
    )
    f.add_trace(
        plotly.graph_objs.Scatter(
            x=d_shelly["logged_at_rounded"],
            y=d_shelly["cum_degree_days"],
            name="degree days",
        ),
        row=3,
        col=1,
    )
    f.add_trace(
        plotly.graph_objs.Scatter(
            x=d_shelly["logged_at_rounded"],
            y=d_shelly["avg_degree_days"],
            showlegend=False,
            line=dict(dash="dot"),
        ),
        row=3,
        col=1,
    )
    f.add_trace(
        plotly.graph_objs.Scatter(
            x=d_gas["measured_at"],
            y=d_gas["reading"] - min(d_gas["reading"]),
            showlegend=False,
            line=dict(dash="dash"),
        ),
        row=3,
        col=1,
        secondary_y=True,
    )
    f.update_layout(
        width=1000,
        height=1000,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        title_text="Temperature (°C)",
    )

    f.write_html("/tmp/shelly.html")
    return f


def get_env_device_ids(conf_path, home):
    conf = from_yaml(conf_path)
    env_ids = {}
    for env in conf["shelly"]:
        if env["home"] == "External":
            env_ids["external"] = env["device_ids"]
        if env["home"] == home:
            env_ids["internal"] = env["device_ids"]
    return env_ids


def main(conf_path, home, shelly_log, meter_log):
    env_ids = get_env_device_ids(conf_path, home)
    d_shelly = load_data(shelly_log, env_ids)
    d_shelly = pivot_on_time(d_shelly, env_ids)
    d_shelly = add_degree_time_columns(d_shelly)
    d_gas = load_gas_data(meter_log)
    plot(d_shelly, d_gas)


if __name__ == "__main__":
    main(
        conf_path=sys.argv[1],
        home=sys.argv[2],
        shelly_log=sys.argv[3],
        meter_log=sys.argv[4],
    )

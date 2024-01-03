import sys

import pandas as pd
import plotly.subplots
import plotly.graph_objs


def load_gas_data(meter_log):
    return pd.read_csv(
        meter_log,
        names=["measured_at", "reading"],
        dtype=dict(
            measured_at=str,
            reading=float,
        ),
        parse_dates=["measured_at"],
    )


def load_shelly_data(shelly_log):
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


def pivot_shelly_data_on_time(d):
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


def add_degree_time_columns(d):
    d_ext = d.copy(deep=True)
    d_ext["cum_degree_days"] = (
        d_ext["temperature_internal"] - d_ext["temperature_external"]
    ).cumsum() / (
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
            "Avg = {:.1f} °C".format(d_shelly["temperature_difference"].mean()),
            "",
        ],
        shared_xaxes=True,
        specs=[[{"secondary_y": True}]] * 3,
    )
    f.add_trace(
        plotly.graph_objs.Scatter(
            x=d_shelly["logged_at_rounded"],
            y=d_shelly["temperature_internal"],
            name="internal",
        ),
        row=1,
        col=1,
    )
    f.add_trace(
        plotly.graph_objs.Scatter(
            x=d_shelly["logged_at_rounded"],
            y=d_shelly["temperature_external"],
            name="external",
        ),
        row=1,
        col=1,
    )
    f.add_trace(
        plotly.graph_objs.Scatter(
            x=d_shelly["logged_at_rounded"],
            y=d_shelly["temperature_difference"],
            name="difference",
        ),
        row=2,
        col=1,
    )
    f.add_trace(
        plotly.graph_objs.Scatter(
            x=d_shelly["logged_at_rounded"],
            y=[d_shelly["temperature_difference"].mean()] * len(d_shelly),
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


def main(shelly_log, meter_log):
    d_shelly = load_shelly_data(shelly_log)
    d_shelly = pivot_shelly_data_on_time(d_shelly)
    d_shelly = add_degree_time_columns(d_shelly)
    d_gas = load_gas_data(meter_log)
    plot(d_shelly, d_gas)


if __name__ == "__main__":
    main(shelly_log=sys.argv[1], meter_log=sys.argv[2])

import sys
from typing import Any, Dict, Tuple

import pandas as pd
from heat.common.utils import from_yaml
from heat.etl.shelly import utils


def density(d_grouped: pd.DataFrame) -> pd.DataFrame:
    def count_numeric_values(l: list) -> int:
        return len([e for e in l if isinstance(e, (int, float))])

    return pd.DataFrame(
        [(t, count_numeric_values(g["temperature"])) for t, g in d_grouped]
    )


def lagged(d: pd.DataFrame, threshold_hours: int) -> pd.DataFrame:
    d = d.assign(lag=d["logged_at"] - d["measured_at"])
    return d[d["lag"] > pd.Timedelta(hours=threshold_hours)]


def main(conf_path: str) -> None:
    conf = from_yaml(conf_path)
    data_dir = conf["local"]["data_dir"]

    d = utils.load_data("{}/log.csv".format(data_dir))
    dg = utils.group_by_time(d)

    density(dg).to_csv("{}/metrics-density.csv".format(data_dir))
    lagged(d, 6).to_csv("{}/metrics-lagged.csv".format(data_dir))


if __name__ == "__main__":
    main(sys.argv[1])

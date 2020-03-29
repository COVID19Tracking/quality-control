from loguru import logger
import pandas as pd
import numpy as np
from datetime import datetime

import udatetime
from data_source import DataSource
from  result_log import ResultLog
import checks


def check_day(df: pd.DataFrame) -> None:

    log = ResultLog()

    for row in df.itertuples():
        checks.total(row, log)
        checks.last_update(row, log)
        checks.positives_rate(row, log)
        checks.death_rate(row, log)
        checks.pendings_rate(row, log)

        ## The recovered column isn't in the API data
        # checks.less_recovered_than_positive(row, log)

    log.print()

def check_timeseries(df: pd.DataFrame) -> None:

    log = ResultLog()

    for state in df["state"].drop_duplicates().values:

        state_df = df.loc[df["state"] == state]
        checks.monotonically_increasing(state_df, log)

    log.print()


def check_working_sheet() -> None:
    """
    Check unpublished results in the working google sheet
    https://docs.google.com/spreadsheets/d/1MvvbHfnjF67GnYUDJJiNYUmGco5KQ9PW0ZRnEP9ndlU/edit#gid=1777138528
    """

    logger.info("--| QUALITY CONTROL --- GOOGLE WORKING SHEET |---------------------------------------------------")

    ds = DataSource()
    check_day(ds.working)


def check_api() -> None:
    """
    Check published
    """
    
    ds = DataSource()

    logger.info("--| QUALITY CONTROL --- HISTORY |-----------------------------------------------------------")
    check_day(ds.history)

    logger.info("--| QUALITY CONTROL --- CURRENT |-----------------------------------------------------------")
    check_timeseries(ds.current)


if __name__ == "__main__":

    check_working_sheet()
    check_api()

from loguru import logger
import pandas as pd
import numpy as np
from datetime import datetime

import udatetime
from sources import GoogleWorksheet
from  result_log import ResultLog
import checks


def check_day(df) -> None:

    log = ResultLog()

    for row in df.itertuples():

        state = row.State
        (error, warning) = checks.last_update(row)
        log.add_results(state, error, warning)

        (error, warning) = checks.positives_rate(row)
        log.add_results(state, error, warning)

        (error, warning) = checks.death_rate(row)
        log.add_results(state, error, warning)

        ## The recovered column isn't in the API data
        # (error, warning) = checks.less_recovered_than_positive(row)
        # log.add_results(error, warning)

        (error, warning) = checks.pendings_rate(row)
        log.add_results(state, error, warning)


    log.print()
    #log.post()

def check_dev_sheet():
    """
    Check data quality within the dev google sheet:
    https://docs.google.com/spreadsheets/d/1MvvbHfnjF67GnYUDJJiNYUmGco5KQ9PW0ZRnEP9ndlU/edit#gid=1777138528
    """

    logger.info("--| QUALITY CONTROL --- GOOGLE DEV SHEET |---------------------------------------------------")

    gs = GoogleWorksheet()
    df = gs.load_dev_from_google()

    check_day(df)


def check_api():
    # get published data from https://covidtracking.com/api/states/daily
    #   this includes all previous dates


    logger.info("--| QUALITY CONTROL --- API ---|-------------------------------------------------------------")

    current_df = pd.read_csv("https://covidtracking.com/api/states.csv")
    current_df["lastUpdateEt"] = pd.to_datetime(current_df["lastUpdateEt"].str.replace(" ", "/2020 "), format="%m/%d/%Y %H:%M")

    check_day(current_df)

    daily_df = pd.read_csv("https://covidtracking.com/api/states/daily.csv")



if __name__ == "__main__":

    check_dev_sheet()
    check_api()

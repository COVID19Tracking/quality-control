from loguru import logger
import pandas as pd
import numpy as np

import udatetime
from sources import GoogleWorksheet
from datetime import datetime

import checks

class ResultsLog():

    def __init__(self):
        self.errors = []
        self.warnings = []

    def add_results(self, error: str, warning: str) -> None:
        self.errors += [error] if error is not None else []
        self.warnings += [warning] if warning is not None else []


def check_day(df) -> None:

    logger.info("==| Internal Consistency Check |==============================================================")
    for row in df.itertuples():
        log = ResultsLog()

        (error, warning) = checks.last_update(row)
        log.add_results(error, warning)

        (error, warning) = checks.positives_rate(row)
        log.add_results(error, warning)

        (error, warning) = checks.death_rate(row)
        log.add_results(error, warning)

        ## The recovered column isn't in the API data
        # (error, warning) = checks.less_recovered_than_positive(row)
        # log.add_results(error, warning)

        (error, warning) = checks.pendings_rate(row)
        log.add_results(error, warning)

        # Print results
        if len(log.errors) == 0 and len(log.warnings) == 0:
            continue
        else:
            logger.warning(f"check {row.state} -->")
            for m in log.errors:
                logger.error(f"   |  {m}")
            for m in log.warnings:
                logger.warning(f"   |  {m}")

    logger.info("")
    logger.info("==| Previous Day Check |======================================================================")



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

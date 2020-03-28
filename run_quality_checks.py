#
# The main check loop
#
from loguru import logger
#from typing import Tuple
import pandas as pd
import numpy as np

import udatetime
from sources import GoogleWorksheet
from datetime import datetime

import checks

class results_log():

    def __init__(self):
        self.errors = []
        self.warnings = []

    def add_results(self, error: str, warning: str) -> None:
        self.errors += [error] if error is not None else []
        self.warnings += [warning] if warning is not None else []


def check_all():

    gs = GoogleWorksheet()

    # get working data from https://docs.google.com/spreadsheets/d/1MvvbHfnjF67GnYUDJJiNYUmGco5KQ9PW0ZRnEP9ndlU/edit#gid=1777138528
    df = gs.load_dev_from_google()

    current_time = udatetime.now_as_eastern()

    logger.info("==| Internal Consistency Check |==============================================================")
    for row in df.itertuples():
        check_one(row, current_time)
    logger.info("")

    logger.info("==| Previous Day Check |==============================================================")

    # get published data from https://covidtracking.com/api/states/daily
    #   this includes all previous dates
    #df_published = gs.load_published_from_api()


def check_one(row, current_time: datetime) -> bool:

    state = row.State
    #logger.info(f"check {state}")
    # print(f"values = {row})

    n_pos, n_neg, n_pending, n_recovered, n_deaths = \
        row.Positive, row.Negative, row.Pending, row.Recovered, row.Deaths
    n_tot = n_pos + n_neg + n_deaths

    log = results_log()

    # ===========================================
    # simple consistency checks

    (error, warning) = checks.last_update(row, current_time)
    log.add_results(error, warning)

    (error, warning) = checks.positives_rate(row, current_time)
    log.add_results(error, warning)

    (error, warning) = checks.death_rate(row, current_time)
    log.add_results(error, warning)

    (error, warning) = checks.less_recovered_than_positive(row, current_time)
    log.add_results(error, warning)

    (error, warning) = checks.pendings_rate(row, current_time)
    log.add_results(error, warning)


    # ===========================================
    # missing data check
    # (note -- this should really check against published results)

    if len(log.errors) == 0 and len(log.warnings) == 0:
        #logger.info(f"check {state} --> OKAY")
        return True

    logger.warning(f"check {row.State} -->")
    for m in log.errors:
        logger.error(f"   |  {m}")
    for m in log.warnings:
        logger.warning(f"   |  {m}")
    return False


if __name__ == "__main__":
    check_all()

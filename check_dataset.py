"""Run Quality Checks against human generated datasets"""

import sys
from loguru import logger
import pandas as pd
import numpy as np
from datetime import datetime
from argparse import ArgumentParser, Namespace, RawDescriptionHelpFormatter

import udatetime
import util

from data_source import DataSource
from result_log import ResultLog
from qc_config import QCConfig
import checks

from forecast_io import load_forecast_hd5
from forecast_plot import plot_to_file

def check_working(ds: DataSource, config: QCConfig) -> ResultLog:
    """
    Check unpublished results in the working google sheet
    https://docs.google.com/spreadsheets/d/1MvvbHfnjF67GnYUDJJiNYUmGco5KQ9PW0ZRnEP9ndlU/edit#gid=1777138528
    """

    logger.info("check working")    
    log = ResultLog()

    # targetDate is the date that the dev sheet is currently working on.
    # phase is what part of their process they are in.
    # targetDateEt is the time that should be used on any 'staleness' checks

    d, phase = checks.current_time_and_phase()

    ds._target_date = d

    df = ds.working
    df["targetDate"] = d.year * 10000 + d.month * 100 + d.day
    df["targetDateEt"] = d
    df["phase"] = phase

    logger.info(f"Running with target date = {d} and phase = {phase}")

    # *** WHEN YOU CHANGE A CHECK THAT IMPACTS WORKING, MAKE SURE TO UPDATE THE EXCEL TRACKING DOCUMENT ***

    for row in df.itertuples():
        try:

            checks.total(row, log)
            #checks.total_tests(row, log)
            checks.last_update(row, log)
            checks.last_checked(row, log)
            checks.checkers_initials(row, log)
            checks.positives_rate(row, log)
            checks.death_rate(row, log)
            checks.less_recovered_than_positive(row, log)
            checks.pendings_rate(row, log)

            df_history = ds.history[ds.history.state == row.state]
            checks.increasing_values(row, df_history, log, check_rate = False)
            checks.expected_positive_increase(row, df_history, log, "working", config)

            df_county_rollup = ds.county_rollup[ds.county_rollup.state == row.state]
            if not df_county_rollup.empty:
                checks.counties_rollup_to_state(row, df_county_rollup, log)

        except Exception as ex:
            logger.exception(ex)
            log.internal_error(row.state, f"{ex}")

    # run loop at end, insted of during run
    if config.plot_models and config.save_results:
        for row in df.itertuples():
            try:
                forecast = load_forecast_hd5(config.results_dir, row.state, row.targetDate)
                if forecast is None:
                    logger.warning(f"Fould not load forecast for {row.state}/{row.targetDate}")
                else:
                    plot_to_file(forecast, f"{config.images_dir}/working", checks.FIT_THRESHOLDS)
            except Exception as ex:
                logger.exception(ex)
                log.internal_error(row.state, f"{ex}")


    return log


def check_current(ds: DataSource, config: QCConfig) -> ResultLog:
    """
    Check the current published results
    """

    logger.info("check current")

    log = ResultLog()

    df = ds.current

    publish_date = 20200331
    logger.error(f" [date is hard-coded to {publish_date}]")

    # setup run settings equivalent to publish date at 5PM
    s = str(publish_date)
    y,m,d = int(s[0:4]), int(s[4:6]), int(s[6:8])
    publish_timestamp = udatetime.naivedatetime_as_eastern(datetime(y, m, d, 12+5))

    ds._target_date = publish_timestamp

    df["targetDate"] = publish_date
    df["targetDateEt"] = publish_timestamp
    df["lastCheckEt"] = df["targetDateEt"]
    df["phase"] = "publish"

    for row in df.itertuples():
        checks.total(row, log)
        checks.last_update(row, log)
        checks.positives_rate(row, log)
        checks.death_rate(row, log)
        checks.pendings_rate(row, log)

        df_history = ds.history[ds.history.state == row.state]
        checks.increasing_values(row, df_history, log, check_rate = False)
        checks.expected_positive_increase(row, df_history, log, "current", config)

        df_county_rollup = ds.county_rollup[ds.county_rollup.state == row.state]
        if not df_county_rollup.empty:
            checks.counties_rollup_to_state(row, df_county_rollup, log)

    return log


def check_history(ds: DataSource) -> ResultLog:
    """
    Check the history
    """

    logger.info("check history")

    df = ds.history

    log = ResultLog()

    for state in df["state"].drop_duplicates().values:
        state_df = df.loc[df["state"] == state]
        checks.monotonically_increasing(state_df, log)

    return log


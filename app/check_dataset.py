"""Run Quality Checks against human generated datasets"""

import sys
from loguru import logger
import pandas as pd
import numpy as np
from datetime import datetime
from argparse import ArgumentParser, Namespace, RawDescriptionHelpFormatter
from datetime import timedelta

import app.checks as checks
from .qc_config import QCConfig
from .data.data_source import DataSource
from .log.result_log import ResultLog
from .modeling.forecast_io import load_forecast_hd5
from .modeling.forecast_plot import plot_to_file
from .util import udatetime
#import .util import 

def is_missing(df: pd.DataFrame) -> bool:
    if df is None: return True
    if df.shape[0] == 0: return True

def check_working(ds: DataSource, config: QCConfig) -> ResultLog:
    """
    Check unpublished results in the working google sheet
    (sheet URL defined in app/data/worksheet_wrapper.py)
    """

    log = ResultLog()

    ds._target_date = config.working_date

    df = ds.working
    if is_missing(df): 
        log.internal("Source", "Working not available")
        return None
    if is_missing(ds.history):
        log.internal("Source", "History not available")
    if is_missing(ds.county_rollup):
        log.internal("Source", "County Rollup not available")

    if not config.is_near_release:
        log.internal("Skip", "Disable Operational checks b/c not near release")

    df["targetDate"] = config.working_date_int
    df["targetDateEt"] = config.working_date

    logger.info(f"Running with working date = {config.working_date_int} and push number = {config.push_num}")

    # *** WHEN YOU CHANGE A CHECK THAT IMPACTS WORKING, MAKE SURE TO UPDATE THE EXCEL TRACKING DOCUMENT ***

    cnt = 0
    for row in df.itertuples():
        try:

            checks.total(row, log)
            #checks.total_tests(row, log)
            checks.last_update(row, log, config)
            checks.last_checked(row, log, config)
            checks.checkers_initials(row, log, config)
            checks.positives_rate(row, log)
            checks.death_rate(row, log)
            checks.less_recovered_than_positive(row, log)
            checks.pendings_rate(row, log)

            if not ds.history is None:
                df_history = ds.history[ds.history.state == row.state]
                has_changed = checks.increasing_values(row, df_history, log, config)
                if has_changed:
                    checks.expected_positive_increase(row, df_history, log, "working", config)

            #checks.delta_vs_cumulative(row, df_history, log, config)

            if not ds.county_rollup is None:
                df_county_rollup = ds.county_rollup[ds.county_rollup.state == row.state]
                if  not df_county_rollup.empty:
                    checks.counties_rollup_to_state(row, df_county_rollup, log)

        except Exception as ex:
            logger.exception(ex)
            log.internal(row.state, f"{ex}")

        if cnt != 0 and cnt % 10 == 0:
            logger.info(f"  processed {cnt} states")
        cnt += 1

    logger.info(f"  processed {cnt} states")

    checks.missing_tests(log)


    # run loop at end, insted of during run
    if config.plot_models and config.save_results:
        cnt = 0
        for row in df.itertuples():
            try:
                forecast = load_forecast_hd5(config.results_dir, row.state, row.targetDate)
                if forecast is None:
                    logger.warning(f"Could not load forecast for {row.state}/{row.targetDate}")
                else:
                    plot_to_file(forecast, f"{config.images_dir}/working", checks.FIT_THRESHOLDS)
            except Exception as ex:
                logger.exception(ex)
                log.internal(row.state, f"{ex}")
        if cnt != 0 and cnt % 10 == 0:
            logger.info(f"  plotted {cnt} states")
        cnt += 1
        logger.info(f"  plotted {cnt} states")

    log.consolidate()
    return log

def check_current(ds: DataSource, config: QCConfig) -> ResultLog:
    """
    Check the current published results
    """

    log = ResultLog()

    df = ds.current
    if is_missing(df): 
        log.internal("Source", "Current not available")
        return None

    if is_missing(ds.history):
        log.internal("Source", "History not available")
    if is_missing(ds.county_rollup):
        log.internal("Source", "County Rollup not available")


    ds._target_date = config.push_date

    df["targetDate"] = config.push_date_int
    df["targetDateEt"] = config.push_date
    df["lastCheckEt"] = config.push_date
    df["push_num"] = config.push_num

    for row in df.itertuples():
        checks.total(row, log)
        checks.last_update(row, log)
        checks.positives_rate(row, log)
        checks.death_rate(row, log)
        checks.pendings_rate(row, log)

        if not ds.history is None:
            df_history = ds.history[ds.history.state == row.state]
            checks.consistent_with_history(row, df_history, log)

        if not ds.history is None:
            df_history = ds.history[ds.history.state == row.state]
            has_changed = checks.increasing_values(row, df_history, log, config)
            if has_changed:
                checks.expected_positive_increase(row, df_history, log, "current", config)

        if not ds.county_rollup is None:
            df_county_rollup = ds.county_rollup[ds.county_rollup.state == row.state]
            if not df_county_rollup.empty:
                checks.counties_rollup_to_state(row, df_county_rollup, log)

    log.consolidate()
    return log


def check_history(ds: DataSource) -> ResultLog:
    """
    Check the history
    """

    log = ResultLog()

    df = ds.history
    if is_missing(df): 
        log.internal("Source", "History not available")
        return None

    for state in df["state"].drop_duplicates().values:
        state_df = df.loc[df["state"] == state]
        checks.monotonically_increasing(state_df, log)

    log.consolidate()
    return log


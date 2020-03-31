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
from  result_log import ResultLog
from forecast import ForecastConfig
import checks

def check_working(ds: DataSource, config: ForecastConfig) -> ResultLog:
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

    df = ds.working
    df["targetDate"] = d.year * 10000 + d.month * 100 + d.day
    df["targetDateEt"] = d
    df["phase"] = phase

    logger.info(f"Running with target date = {d} and phase = {phase}")

    # *** WHEN YOU CHANGE A CHECK THAT IMPACTS WORKING, MAKE SURE TO UPDATE THE EXCEL TRACKING DOCUMENT ***

    for row in df.itertuples():
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
        checks.increasing_values(row, df_history, log)
        checks.expected_positive_increase(row, df_history, log, "working", config)

    return log


def check_current(ds: DataSource, config: ForecastConfig) -> ResultLog:
    """
    Check the current published results
    """

    logger.info("check current")

    log = ResultLog()

    df = ds.current

    publish_date = 20200330
    logger.error(f" [date is hard-coded to {publish_date}]")

    # setup run settings equivalent to publish date at 5PM
    s = str(publish_date)
    y,m,d = int(s[0:4]), int(s[4:6]), int(s[6:8])

    df["targetDate"] = publish_date
    df["targetDateEt"] = udatetime.naivedatetime_as_eastern(datetime(y, m, d, 12+5))
    df["lastCheckEt"] = df["targetDateEt"]
    df["phase"] = "publish"

    for row in df.itertuples():
        checks.total(row, log)
        checks.last_update(row, log)
        checks.positives_rate(row, log)
        checks.death_rate(row, log)
        checks.pendings_rate(row, log)

        df_history = ds.history[ds.history.state == row.state]
        checks.increasing_values(row, df_history, log)
        checks.expected_positive_increase(row, df_history, log, "current", config)

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

def load_args_parser(config) -> ArgumentParser:
    " load arguments parser "

    parser = ArgumentParser(
        description=__doc__,
        formatter_class=RawDescriptionHelpFormatter)

    parser.add_argument('state', metavar='state', type=str, nargs='*',
        help='states to check')

    parser.add_argument(
        '-w', '--working', dest='check_working', action='store_true', default=False,
        help='check the working results (only)')

    parser.add_argument(
        '-d', '--daily', dest='check_daily', action='store_true', default=False,
        help='check the daily results (only)')

    parser.add_argument(
        '-x', '--history', dest='check_history', action='store_true', default=False,
        help='check the history (only)')

    plot_models = config["MODEL"]["plot_models"] == "True"

    parser.add_argument(
        '--plot', dest='plot_models', action='store_true', default=plot_models,
        help='plot the model curves')

    parser.add_argument(
        '--images_dir',
        default=config["MODEL"]["images_dir"],
        help='directory for model curves')

    return parser

def main() -> None:

    # pylint: disable=no-member

    config = util.read_config_file("quality-control")
    parser = load_args_parser(config)
    args = parser.parse_args(sys.argv)

    if not args.check_working and not args.check_daily and not args.check_history:
        logger.info("  [default to all sources]")
        args.check_working = True
        args.check_daily = True
        args.check_history = True

    config = ForecastConfig(images_dir=args.images_dir, plot_models=args.plot_models)
    if config.plot_models:
        logger.warning(f"  [save forecast curves to {args.images_dir}]")

    if len(args.state) != 0:
        logger.error("  [states filter not implemented]")

    ds = DataSource()

    if args.check_working:
        logger.info("--| QUALITY CONTROL --- GOOGLE WORKING SHEET |---------------------------------------------------")
        log = check_working(ds, config=config)
        log.print()

    if args.check_daily:
        logger.info("--| QUALITY CONTROL --- CURRENT |-----------------------------------------------------------")
        log = check_current(ds, config=config)
        log.print()

    if args.check_history:
        logger.info("--| QUALITY CONTROL --- HISTORY |-----------------------------------------------------------")
        log = check_history(ds)
        log.print()


if __name__ == "__main__":
    main()

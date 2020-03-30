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

def check_working(ds: DataSource) -> ResultLog:
    """
    Check unpublished results in the working google sheet
    https://docs.google.com/spreadsheets/d/1MvvbHfnjF67GnYUDJJiNYUmGco5KQ9PW0ZRnEP9ndlU/edit#gid=1777138528
    """

    log = ResultLog()

    target_date = 20200329 # hard code for now - josh

    for row in ds.working.itertuples():
        checks.total(row, log)
        checks.last_update(row, log)
        checks.positives_rate(row, log)
        checks.death_rate(row, log)
        checks.less_recovered_than_positive(row, log)
        checks.pendings_rate(row, log)

        df_history = ds.history[ds.history.state == row.state]
        checks.increasing_values(row, target_date, df_history, log)

    return log


def check_current(ds: DataSource) -> ResultLog:
    """
    Check the current published results
    """

    log = ResultLog()

    target_date = 20200329 # hard code for now - josh

    for row in ds.current.itertuples():
        checks.total(row, log)
        checks.last_update(row, log)
        checks.positives_rate(row, log)
        checks.death_rate(row, log)
        checks.pendings_rate(row, log)

        df_history = ds.history[ds.history.state == row.state]

        checks.increasing_values(row, target_date, df_history, log)

    return log


def check_history(ds: DataSource, config: ForecastConfig) -> ResultLog:
    """
    Check the history
    """

    df = ds.history

    log = ResultLog()

    for state in df["state"].drop_duplicates().values:
        state_df = df.loc[df["state"] == state]
        checks.monotonically_increasing(state_df, log)
        checks.expected_positive_increase(state_df, log, config)

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
        log = check_working(ds)
        log.print()

    if args.check_daily:
        logger.info("--| QUALITY CONTROL --- CURRENT |-----------------------------------------------------------")
        log = check_current(ds)
        log.print()


    if args.check_history:
        logger.info("--| QUALITY CONTROL --- HISTORY |-----------------------------------------------------------")
        log = check_history(ds, config=config)
        log.print()


if __name__ == "__main__":
    main()

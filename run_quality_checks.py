"""Run Quality Checks against human generated datasets"""

import sys
from loguru import logger
import pandas as pd
import numpy as np
from datetime import datetime
from argparse import ArgumentParser, Namespace, RawDescriptionHelpFormatter

import udatetime
from data_source import DataSource
from  result_log import ResultLog
import checks

def check_working(ds: DataSource) -> None:
    """
    Check unpublished results in the working google sheet
    https://docs.google.com/spreadsheets/d/1MvvbHfnjF67GnYUDJJiNYUmGco5KQ9PW0ZRnEP9ndlU/edit#gid=1777138528
    """

    log = ResultLog()

    for row in ds.working.itertuples():
        checks.total(row, log)
        checks.last_update(row, log)
        checks.positives_rate(row, log)
        checks.death_rate(row, log)
        checks.less_recovered_than_positive(row, log)
        checks.pendings_rate(row, log)

        df_history = ds.history[ds.history.state == row.state]
        checks.increasing_values(row, df_history, log, offset=0)

    log.print()


def check_current(ds: DataSource) -> None:
    """
    Check the current published results
    """

    log = ResultLog()

    for row in ds.working.itertuples():
        checks.total(row, log)
        checks.last_update(row, log)
        checks.positives_rate(row, log)
        checks.death_rate(row, log)
        checks.pendings_rate(row, log)

        df_history = ds.history[ds.history.state == row.state]

        checks.increasing_values(row, df_history, log, offset=1)

    log.print()


def check_history(df: pd.DataFrame) -> None:
    """
    Check the history
    """

    log = ResultLog()

    for state in df["state"].drop_duplicates().values:

        state_df = df.loc[df["state"] == state]
        checks.monotonically_increasing(state_df, log)

    log.print()

def load_args_parser() -> ArgumentParser:
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
    
    return parser

def main() -> None:

    # pylint: disable=no-member

    parser = load_args_parser()
    args = parser.parse_args(sys.argv)

    if not args.check_working and not args.check_daily and not args.check_history:
        logger.info("  [default to working only]")
        args.check_working = True

    ds = DataSource()

    if args.check_working:
        logger.info("--| QUALITY CONTROL --- GOOGLE WORKING SHEET |---------------------------------------------------")
        check_working(ds)
    
    if args.check_daily:
        logger.info("--| QUALITY CONTROL --- CURRENT |-----------------------------------------------------------")
        check_current(ds.history)

    if args.check_history:
        logger.info("--| QUALITY CONTROL --- HISTORY |-----------------------------------------------------------")
        check_history(ds.current)


if __name__ == "__main__":
    main()

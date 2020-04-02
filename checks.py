#
# Individual Check Routines
#
#   Each routine checks a specific aspect of a single state
#
#   If any issues are found, the check routine calls the log to report it.
#   Each message has a category.  See ResultLog for a list of categories.
#
#   Each row has a 'phase' appended to it that can be used to control how
#   errors are reported.  At certain times, fields like checked are expected
#   to be cleared.  This should mainly be used with operational cheks.
#
# To add a new check:
#    1. create the routine here
#    2. add it to the calling routines in check_dataset
#    3. put it behind the experimental flag (config.enable_experimental) at first
#    4. WHEN YOU CHANGE A CHECK THAT IMPACTS WORKING, MAKE SURE TO UPDATE THE EXCEL TRACKING DOCUMENT
#


from datetime import datetime
from loguru import logger
import pandas as pd
import numpy as np
from typing import Tuple

import udatetime
from result_log import ResultLog
from forecast import Forecast
from qc_config import QCConfig

from forecast_plot import plot_to_file
from forecast_io import save_forecast_hd5, load_forecast_hd5

START_OF_TIME = udatetime.naivedatetime_as_eastern(datetime(2020,1,2))


def current_time_and_phase() -> Tuple[datetime, str]:
    "get the current time (ET) and phase of the process given the hour"

    target_time = udatetime.now_as_eastern()

    hour = target_time.hour

    # note -- these are all just guesses on a mental model of 1 update per day. Josh
    phase = ""
    if hour < 10:
        phase = "inactive"
    elif hour < 12 + 2:
        phase = "prepare" # preparing for run
    elif hour < 12 + 4:
        phase ="active" # working on an update
    elif hour < 12 + 6:
        phase ="publish" # getting close to publishing
    elif hour < 12 + 9:
        phase ="cleanup" # cleanup from main run
    elif hour < 12 + 10:
        phase ="update" # updating numbers for the day
    else:
        phase = "inactive"

    return target_time, phase


# ----------------------------------------------------------------

def total(row, log: ResultLog):
    """Check that pendings, positive, and negative sum to the reported total"""

    n_pos, n_neg, n_pending, n_death, n_tot = \
        row.positive, row.negative, row.pending, row.death, row.total

    n_diff = n_tot - (n_pos + n_neg + n_pending)
    if n_pos == -1000:
        log.operational(row.state, f"Data Entry Error on positive")
    elif n_neg == -1000:
        log.operational(row.state, f"Data Entry Error on negative")
    elif n_pending == -1000:
        log.operational(row.state, f"Data Entry Error on pending")
    elif n_death == -1000:
        log.operational(row.state, f"Data Entry Error on death")
    elif n_tot == -1000:
        log.operational(row.state, f"Data Entry Error on total")
    elif n_diff != 0:
        log.operational(row.state, f"Formula broken -> Positive ({n_pos}) + Negative ({n_neg}) + Pending ({n_pending}) != Total ({n_tot}), delta = {n_diff}")

def total_tests(row, log: ResultLog):
    """Check that positive, and negative sum to the reported totalTest"""

    # note -- I don't know where this field is in the sheet so this test is not used right now - Josh

    n_pos, n_neg, n_tests = \
        row.positive, row.negative, row.totalTestResults

    n_diff = n_tests - (n_pos + n_neg)
    if n_diff != 0:
        log.operational(row.state, f"Formula broken -> Positive ({n_pos}) + Negative ({n_neg}) != Total Tests ({n_tests}), delta = {n_diff}")


def last_update(row, log: ResultLog):
    """Source has updated within a reasonable timeframe"""

    updated_at = row.lastUpdateEt.to_pydatetime()
    target_time = row.targetDateEt.to_pydatetime()
    delta = target_time - updated_at
    days = delta.total_seconds() / (24 * 60.0 * 60)

    if days >= 1.5:
        log.data_source(row.state, f"source hasn't updated in {days:.1f} days")
    #elif hours > 18.0:
    #   log.data_source(row.state, f"Last Updated (col T) hasn't been updated in {hours:.0f}  hours")

def last_checked(row, log: ResultLog):
    """Data was checked within a reasonable timeframe"""

    target_date = row.targetDateEt.to_pydatetime()
    updated_at = row.lastUpdateEt.to_pydatetime()
    checked_at = row.lastCheckEt.to_pydatetime()

    if checked_at <= START_OF_TIME:
        phase = row.phase
        if phase == "inactive":
            pass
        elif phase in ["publish", "update"]:
            log.operational(row.state, f"last check ET (column AK) is blank")
        elif phase in ["prepare", "cleanup"]:
            pass
        return

    delta = updated_at - checked_at
    hours = delta.total_seconds() / (60.0 * 60)
    if hours > 1.0:
        s_updated = updated_at.strftime('%m/%d %H:%M')
        s_checked = checked_at.strftime('%m/%d %H:%M')
        log.operational(row.state, f"Last Check ET (column AJ) is {s_checked} which is less than Last Update ET (column AI)  {s_updated} by {hours:.0f} hours")
        return

    delta = target_date - checked_at
    hours = delta.total_seconds() / (60.0 * 60)
    if hours > 6.0:
        s_checked = checked_at.strftime('%m/%d %H:%M')
        log.operational(row.state, f"Last Check ET (column AJ) has not been updated in {hours:.0f} hours ({s_checked} by {row.checker})")
        return


def checkers_initials(row, log: ResultLog):
    """Confirm that checker initials are records"""

    phase = row.phase
    if phase == "inactive": return

    target_date = row.targetDateEt.to_pydatetime()
    checked_at = row.lastCheckEt.to_pydatetime()
    if checked_at <= START_OF_TIME: return

    is_near_release = phase in ["publish", "update"]

    checker = row.checker.strip()
    doubleChecker = row.doubleChecker.strip()

    delta_hours = (target_date - checked_at).total_seconds() / (60.0 * 60.0)

    if checker == "":
        if 0 < delta_hours < 5:
            s_checked = checked_at.strftime('%m/%d %H:%M')
            log.operational(row.state, f"missing checker initials (column AK) but checked date set recently (at {s_checked})")
        elif is_near_release:
            log.operational(row.state, f"missing checker initials (column AK)")
        return
    if doubleChecker == "":
        if is_near_release:
            log.operational(row.state, f"missing double-checker initials (column AL)")
        return

    #elif hours > 18.0:
    #   log.data_source(row.state, f"Last Updated (col T) hasn't been updated in {hours:.0f}  hours")


def positives_rate(row, log: ResultLog):
    """Check that positives compose <20% test results"""

    n_pos, n_neg = row.positive, row.negative
    n_tot = n_pos + n_neg

    percent_pos = 100.0 * n_pos / n_tot if n_tot > 0 else 0.0
    if n_tot > 100:
        if percent_pos > 40.0 and n_pos > 20:
            log.data_quality(row.state, f"high positives rate {percent_pos:.0f}% (positive={n_pos:,}, total={n_tot:,})")
    else:
        if percent_pos > 80.0 and n_pos > 20:
            log.data_quality(row.state, f"high positives rate {percent_pos:.0f}% (positive={n_pos:,}, total={n_tot:,})")

def death_rate(row, log: ResultLog):
    """Check that deaths are <5% of test results"""

    n_pos, n_neg, n_deaths = row.positive, row.negative, row.death
    n_tot = n_pos + n_neg

    percent_deaths = 100.0 * n_deaths / n_tot if n_tot > 0 else 0.0
    if n_tot > 100:
        if percent_deaths > 5.0:
            log.data_quality(row.state, f"high death rate {percent_deaths:.0f}% (positive={n_deaths:,}, total={n_tot:,})")
    else:
        if percent_deaths > 10.0:
            log.data_quality(row.state, f"high death rate {percent_deaths:.0f}% (positive={n_deaths:,}, total={n_tot:,})")


def less_recovered_than_positive(row, log: ResultLog):
    """Check that we don't have more recovered than positive"""

    if row.recovered > row.positive:
        log.data_quality(row.state, f"More recovered than positive (recovered={row.recovered:,}, positive={row.positive:,})")


def pendings_rate(row, log: ResultLog):
    """Check that pendings are not more than 20% of total"""

    n_pos, n_neg, n_pending = row.positive, row.negative, row.pending
    n_tot = n_pos + n_neg
    percent_pending = 100.0 * n_pending / n_tot if n_tot > 0 else 0.0

    if n_tot > 1000:
        if percent_pending > 20.0:
            log.data_quality(row.state, f"high pending rate {percent_pending:.0f}% (pending={n_pending:,}, total={n_tot:,})")
    else:
        if percent_pending > 80.0:
            log.data_quality(row.state, f"high pending rate {percent_pending:.0f}% (pending={n_pending:,}, total={n_tot:,})")


# ----------------------------------------------------------------

COUNTY_ERROR_THRESHOLDS = {
    "positive-small": (.75, 1.3),
    "positive-large": (.75, 1.2),
    "death-small": (.75, 1.4),
    "death-large": (.75, 1.3),
}

def counties_rollup_to_state(row, counties: pd.DataFrame, log: ResultLog):
    """
    Check that county totals from NYT, CSBS, CDS datasets are
 about equal to the reported state totals. Metrics compared are:
        - positive cases
        - patient deaths
    """

    df = counties.copy()

    t = "positive-small" if row.positive < 500 else "positive-large"
    thresholds = COUNTY_ERROR_THRESHOLDS[t]
    df["c"] = row.positive
    df["c_min"] = (thresholds[0] * df["cases"]).astype(np.int)
    df["c_max"] = (thresholds[1] * df["cases"] + 10).astype(np.int)
    df["c_ok"] = (df.c_min <= df.c) & (df.c <= df.c_max)

    t = "death-small" if row.death < 50 else "death-large"
    thresholds = COUNTY_ERROR_THRESHOLDS[t]
    df["d"] = row.death
    df["d_min"] = (thresholds[0] * df["deaths"]).astype(np.int)
    df["d_max"] = (thresholds[1] * df["deaths"] + 10).astype(np.int)
    df["d_ok"] = (df.d_min <= df.d) & (df.d <= df.d_max)

    # use last (biggest value)
    #index = (df.shape[0] // 2)
    xindex = df.shape[0] - 1

    if row.positive > 100:
        df.sort_values(by="cases", inplace=True)
        mid = df.iloc[xindex]
        if not mid.c_ok:
            print(f"  positive ({row.positive:,}) does not match county aggregate ({mid.c_min:,} to {mid.c_max:,})")
            log.data_quality(row.state, f"positive ({row.positive:,}) does not match county aggregate ({mid.cases:,}, allow {mid.c_min:,} to {mid.c_max:,})")

    if row.death > 20:
        df.sort_values(by="deaths", inplace=True)
        mid = df.iloc[xindex]
        if not mid.d_ok:
            print(f"  death ({row.death:,}) does not match county aggregate ({mid.d_min:,} to {mid.d_max:,})")
            log.data_quality(row.state, f"death ({row.death:,}) does not match county aggregate ({mid.deaths:,}, allow {mid.d_min:,} to {mid.d_max:,})")

# ----------------------------------------------------------------

IGNORE_THRESHOLDS = {
    "positive": 100,
    "negative": 900,
    "death": 20,
}

def days_since_change(val, vec_vals: pd.Series, vec_date) -> Tuple[int, int]:

    vals = vec_vals.values
    for i in range(len(vals)):
        if vals[i] != val: return i+1, vec_date.values[i]
    return -1, None

#TODO: add date to dev worksheet so we don't have to pass it around

def increasing_values(row, df: pd.DataFrame, log: ResultLog, config: QCConfig = None):
    """Check that new values more than previous values

    df contains the historical values (newest first).  offset controls how many days to look back.
    consolidate lines if everything changed
    """

    if not config: config = QCConfig()

    df = df[df.date < row.targetDate]

    last_updated = row.lastUpdateEt
    d_updated = last_updated.year * 10000 + last_updated.month * 100 + last_updated.day
    d_last_change = 20200101

    dict_row = row._asdict()

    debug = config.enable_debug

    is_same_messages = []
    consolidate, n_days, n_days_prev = True, -1, 0
    for c in ["positive", "negative", "death"]:
        if debug: logger.debug(f"check {row.state} {c}")
        val = dict_row[c]
        vec = df[c].values
        prev_val = vec[0] if vec.size > 0 else 0

        if val < prev_val:
            log.data_quality(row.state, f"{c} ({val:,}) decreased, prior value is {prev_val:,}")
            consolidate = False
            continue

        # allow value to be the same if below a threshold
        if val < IGNORE_THRESHOLDS[c]: continue

        phase = row.phase
        checked_at = row.lastCheckEt.to_pydatetime()
        is_check_field_set = checked_at > START_OF_TIME

        if val == -1000:
            log.operational(row.state, f"{c} value cannot be converted to a number")
            consolidate = False
            continue

        if val == prev_val:
            n_days, d = days_since_change(val, df[c], df["date"])
            if n_days >= 0:
                d_last_change = max(d_last_change, d)

                if prev_val >= 20 and (is_check_field_set or phase in ["publish", "update"]):
                    sd = str(d)
                    sd = sd[4:6] + "/" + sd[6:8]
                    is_same_messages.append(f"{c} ({val:,}) hasn't changed since {sd} ({n_days} days)")
                    if debug: logger.debug(f"{c} ({val:,}) hasn't changed since {sd} ({n_days} days)")

                if n_days_prev == 0:
                    n_days_prev = n_days
                elif n_days_prev != n_days:
                    if debug: logger.debug(f"{c} ({val:,}) changed {n_days} days ago -> force individual lines ")
                    consolidate = False
            else:
                d_last_change = max(d_last_change, df["date"].values[-1])
                log.data_source(row.state, f"{c} ({val:,}) constant for all time")
                if debug: logger.debug(f"{c} ({val:,}) constant -> force individual lines ")
                consolidate = False
            continue

    if d_last_change == 20200101:
        return

    # alert if local time appears to updated incorrectly
    elif d_last_change != d_updated:
        sd = str(d_last_change)
        sd = sd[4:6] + "/" + sd[6:8]
        sd_updated = f"{last_updated.month}/{last_updated.day} {last_updated.hour:02}:{last_updated.minute:02}"
        log.operational(row.state, f"local time (column V) set to {sd_updated} but values haven't changed since {sd} ({n_days} days)")

    # only show one message if all values are same
    if  consolidate:
        sd = str(d_last_change)
        sd = sd[4:6] + "/" + sd[6:8]
        log.data_source(row.state, f"positive/negative/deaths haven't changed since {sd} ({n_days} days)")
    else:
        for m in is_same_messages: log.data_source(row.state, m)


# ----------------------------------------------------------------

def monotonically_increasing(df: pd.DataFrame, log: ResultLog):
    """Check that timeseries values are monotonically increasing

    Input is expected to be the values for a single state
    """

    columns_to_check = ["positive", "negative","hospitalized", "death"]

    state = df["state"].min()
    if state != df["state"].max():
        raise Exception("Expected input to be for a single state")

    # TODO: don't group on state -- this is already filtered to a single state
    df = df.sort_values(["state", "date"], ascending=True)
    df_lagged = df.groupby("state")[columns_to_check] \
        .shift(1) \
        .rename(columns=lambda c: c+"_lag")

    df_comparison = df.merge(df_lagged, left_index=True, right_index=True, how="left")

    # check that all the counts are >= the previous day
    for col in columns_to_check:
        if (df_comparison[f"{col}_lag"] > df_comparison[col]).any():
            error_dates = df_comparison.loc[(df_comparison[f"{col}_lag"] > df_comparison[col])]["date"]
            error_dates_str = error_dates.astype(str).str.cat(sep=", ")

            log.data_quality(state, f"{col} values decreased from the previous day (on {error_dates_str})")

# ----------------------------------------------------------------

FIT_THRESHOLDS = [0.9, 1.2]

def expected_positive_increase( row, history: pd.DataFrame,
                                log: ResultLog, context: str, config: QCConfig=None):
    """
    Fit state-level daily positives data to an exponential and a linear curve.
    Get expected vs actual case increase to determine if current positives
    are within the expected ranges.

    The exponential is used as the upper bound. The linear is used as the lower bound.

    TODO: Eventually these curves will NOT be exp (perhaps logistic?)
          Useful to know which curves have been "leveled" but from a
          data quality perspective, this check would become annoying
    """

    if not config: config = QCConfig()

    current = row # this is an iterrows() record, not a data frame

    forecast = Forecast()
    forecast.date = current.targetDate

    history = history.loc[history["date"] != forecast.date]

    forecast.fit(history)
    forecast.project(current)

    if config.save_results:
        save_forecast_hd5(forecast, config.results_dir)
    elif config.plot_models:
        plot_to_file(forecast, f"{config.images_dir}/{context}", FIT_THRESHOLDS)

    actual_value, expected_linear, expected_exp = forecast.results

    # --- sanity checks ----
    debug = config.enable_debug
    if debug: logger.debug(f"{forecast.state}: actual = {actual_value:,}, linear={expected_linear:,}, exp={expected_exp:,}")

    is_bad = False
    if 100 < expected_linear > 100_000:
        logger.error(f"{forecast.state}: actual = {actual_value:,}, linear model = {expected_linear:,} ")
        log.internal_error(forecast.state, f"actual = {actual_value:,}, linear model = {expected_linear:,} ")
        is_bad = True
    if 100 < expected_linear > 100_000:
        logger.error(f"{forecast.state}: actual = {actual_value:,}, exponental model = {expected_exp:,} ")
        log.internal_error(forecast.state, f"actual = {actual_value:,}, exponental model = {expected_exp:,} ")
        is_bad = True
    if (not is_bad) and (expected_linear >= expected_exp):
        logger.error(f"{forecast.state}: actual = {actual_value:,}, linear model ({expected_linear:,}) > exponental model ({expected_exp:,})")
        log.internal_error(forecast.state, f"actual = {actual_value:,}, linear model ({expected_linear:,}) > exponental model ({expected_exp:,})")
        is_bad = True

    if is_bad:
        logger.error(f"{forecast.state}: fit\n{history[['date', 'positive','total']]}")
        logger.error(f"{forecast.state}: project {current.targetDate} positive={current.positive:,}, total={current.total:,}")
    elif debug:
        logger.debug(f"{forecast.state}: fit\n{history[['date', 'positive','total']]}")
        logger.debug(f"{forecast.state}: project {current.targetDate} positive={current.positive:,}, total={current.total:,}")

    if is_bad: return
    # -----

    min_value = int(FIT_THRESHOLDS[0] * expected_linear)
    max_value = int(FIT_THRESHOLDS[1] * expected_exp)

    # limit to N>=100
    if actual_value < 100: return

    m, d = str(forecast.date)[4:6],str(forecast.date)[6:]
    sd = f"for {m}/{d}" if config.show_dates else ""

    if not (min_value <= actual_value <=  max_value):

        if actual_value < expected_linear:
            log.data_quality(forecast.state, f"positive ({actual_value:,}){sd} decelerated beyond linear trend, expected > {min_value:,}")
        else:
            log.data_quality(forecast.state, f"positive ({actual_value:,}){sd} accelerated beyond exponential trend, expected < {max_value:,}")

    # if the linear projection is steeper than the exp let's
    # check that the value is somewhat similar to the linear projection
    # (linear model * threshold)
    elif min_value >= max_value:

        high_linear = int(FIT_THRESHOLDS[1] * expected_linear)
        low_linear = int(expected_linear - (high_linear - expected_linear))

        if not (low_linear <= actual_value <= high_linear):

            if actual_value < low_linear:
                log.data_quality(forecast.state, f"positive ({actual_value:,}){sd} decelerated beyond linear trend, expected > {low_linear:,}")
            else:
                log.data_quality(forecast.state, f"positive ({actual_value:,}){sd} accelerated beyond exponential trend, expected < {high_linear:,}")

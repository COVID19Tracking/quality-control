#
# Individual Check Routines
#
#   Each routine checks a specific aspect of a single state
#
#   If any issues are found, the check routine calls the log to report it.
#   The three log methods are info/warning/error.
#
#   Each row has a 'phase' appended to it that can be used to control how
#   errors are reported.  At certain times, fields like checked are expected
#   to be cleared.
#
# *** WHEN YOU CHANGE A CHECK THAT IMPACTS WORKING, MAKE SURE TO UPDATE THE EXCEL TRACKING DOCUMENT ***


from datetime import datetime
import pandas as pd
import numpy as np
from typing import Tuple

import udatetime
from result_log import ResultLog
from forecast import Forecast, ForecastConfig

from forecast_plot import plot_to_file

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

    n_pos, n_neg, n_pending, n_tot = \
        row.positive, row.negative, row.pending, row.total

    n_diff = n_tot - (n_pos + n_neg + n_pending)
    if n_diff != 0:
        log.error(row.state, f"Formula broken -> Postive ({n_pos}) + Negative ({n_neg}) + Pending ({n_pending}) != Total ({n_tot}), delta = {n_diff}")

def total_tests(row, log: ResultLog):
    """Check that positive, and negative sum to the reported totalTest"""

    # note -- I don't know where this field is in the sheet so this test is not used right now - Josh

    n_pos, n_neg, n_tests = \
        row.positive, row.negative, row.totalTestResults

    n_diff = n_tests - (n_pos + n_neg)
    if n_diff != 0:
        log.error(row.state, f"Formula broken -> Postive ({n_pos}) + Negative ({n_neg}) != Total Tests ({n_tests}), delta = {n_diff}")


def last_update(row, log: ResultLog):
    """Source has updated within a reasonable timeframe"""

    updated_at = row.lastUpdateEt.to_pydatetime()
    target_time = row.targetDateEt.to_pydatetime()
    delta = target_time - updated_at
    days = delta.total_seconds() / (24 * 60.0 * 60)

    if days >= 1.5:
        log.error(row.state, f"source hasn't updated in {days:.1f} days")
    #elif hours > 18.0:
    #   log.error(row.state, f"Last Updated (col T) hasn't been updated in {hours:.0f}  hours")

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
            log.error(row.state, f"check needed")
        elif phase in ["prepare", "cleanup"]:
            log.info(row.state, f"check needed")
        return

    delta = updated_at - checked_at
    hours = delta.total_seconds() / (60.0 * 60)
    if hours > 2.0:
        s_updated = updated_at.strftime('%m/%d %H:%M')
        s_checked = checked_at.strftime('%m/%d %H:%M')
        log.error(row.state, f"updated since last check: {hours:.0f} hours ago at {s_updated}, checked at {s_checked}")
        return

    delta = target_date - updated_at
    hours = delta.total_seconds() / (60.0 * 60)
    if hours > 12.0:
        s_checked = checked_at.strftime('%m/%d %H:%M')
        log.warning(row.state, f"source has not been checked in {hours:.0f} hours at {s_checked}")
        return

    #elif hours > 18.0:
    #   log.error(row.state, f"Last Updated (col T) hasn't been updated in {hours:.0f}  hours")


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
            log.error(row.state, f"missing checker initials but checked date set recently (at {s_checked})")
        elif is_near_release:
            log.error(row.state, f"missing checker initials")
        else:
            log.info(row.state, f"missing checker initials")
        return
    if doubleChecker == "":
        if is_near_release:
            log.error(row.state, f"missing double-checker initials")
        else:
            log.info(row.state, f"Missing double-checker initials")
        return

    #elif hours > 18.0:
    #   log.error(row.state, f"Last Updated (col T) hasn't been updated in {hours:.0f}  hours")


def positives_rate(row, log: ResultLog):
    """Check that positives compose <20% test results"""

    n_pos, n_neg, n_deaths = row.positive, row.negative, row.death
    n_tot = n_pos + n_neg

    percent_pos = 100.0 * n_pos / n_tot if n_tot > 0 else 0.0
    if n_tot > 100:
        if percent_pos > 40.0 and n_pos > 20:
            log.error(row.state, f"Too many positive {percent_pos:.0f}% (positive={n_pos:,}, total={n_tot:,})")
    else:
        if percent_pos > 80.0 and n_pos > 20:
            log.error(row.state, f"Too many positive {percent_pos:.0f}% (positive={n_pos:,}, total={n_tot:,})")

def death_rate(row, log: ResultLog):
    """Check that deaths are <5% of test results"""

    n_pos, n_neg, n_deaths = row.positive, row.negative, row.death
    n_tot = n_pos + n_neg

    percent_deaths = 100.0 * n_deaths / n_tot if n_tot > 0 else 0.0
    if n_tot > 100:
        if percent_deaths > 5.0:
            log.error(row.state, f"Too many deaths {percent_deaths:.0f}% (positive={n_deaths:,}, total={n_tot:,})")
    else:
        if percent_deaths > 10.0:
            log.error(row.state, f"Too many deaths {percent_deaths:.0f}% (positive={n_deaths:,}, total={n_tot:,})")


def less_recovered_than_positive(row, log: ResultLog):
    """Check that we don't have more recovered than positive"""

    if row.recovered > row.positive:
        log.error(row.state, f"More recovered than positive (recovered={row.recovered:,}, positive={row.positive:,})")


def pendings_rate(row, log: ResultLog):
    """Check that pendings are not more than 20% of total"""

    n_pos, n_neg, n_pending = row.positive, row.negative, row.pending
    n_tot = n_pos + n_neg
    percent_pending = 100.0 * n_pending / n_tot if n_tot > 0 else 0.0

    if n_tot > 1000:
        if percent_pending > 20.0:
            log.warning(row.state, f"too many pending {percent_pending:.0f}% (pending={n_pending:,}, total={n_tot:,})")
    else:
        if percent_pending > 80.0:
            log.warning(row.state, f"too many pending {percent_pending:.0f}% (pending={n_pending:,}, total={n_tot:,})")


# ----------------------------------------------------------------

COUNTY_ERROR_THRESHOLDS = {
    "positive": .1,
    "death": .2
}

def counties_rollup_to_state(row, counties: pd.DataFrame, log: ResultLog):
    """
    Check that county totals from NYT, CSBS, CDS datasets are
    about equal to the reported state totals. Metrics compared are:
        - positive cases
        - patient deaths
    """
    if row.positive > 20:
        pos_error =  abs(counties["cases"] - row.positive).min() / row.positive
        if pos_error > COUNTY_ERROR_THRESHOLDS["positive"]:
            closest_pos = int(round(pos_error * row.positive + row.positive))
            log.error(row.state, f"county aggregate for positive tests does not match state totals (state: {row.positive}, county: {closest_pos})")

    if row.death > 20:
        death_error = abs(counties["deaths"] - row.death).min() / row.death
        if death_error > COUNTY_ERROR_THRESHOLDS["death"]:
            closest_death = int(round(death_error * row.death + row.death))
            log.error(row.state, f"county aggregate for patient deaths does not match state totals (state: {row.death}, county: {closest_death})")


# ----------------------------------------------------------------

IGNORE_THRESHOLDS = {
    "positive": 100,
    "negative": 900,
    "death": 20,
    "total": 1000
}
EXPECTED_PERCENT_THRESHOLDS = {
    "positive": (5,40),
    "negative": (5,50),
    "death": (0,10),
    "total": (5,50)
}

def days_since_change(val, vec_vals: pd.Series, vec_date) -> Tuple[int, int]:

    vals = vec_vals.values
    for i in range(len(vals)):
        if vals[i] != val: return i+1, vec_date.values[i]
    return -1, None

#TODO: add date to dev worksheet so we don't have to pass it around

def increasing_values(row, df: pd.DataFrame, log: ResultLog):
    """Check that new values more than previous values

    df contains the historical values (newest first).  offset controls how many days to look back.
    """

    df = df[df.date < row.targetDate]

    #print(df)
    #exit(-1)

    dict_row = row._asdict()

    for c in ["positive", "negative", "death", "total"]:
        val = dict_row[c]
        vec = df[c].values
        prev_val = vec[0] if vec.size > 0 else 0

        if val < prev_val:
            log.error(row.state, f"{c} value ({val:,}) is less than prior value ({prev_val:,})")

        # allow value to be the same if below a threshold
        if val < IGNORE_THRESHOLDS[c]: continue

        phase = row.phase
        checked_at = row.lastCheckEt.to_pydatetime()
        is_check_field_set = checked_at > START_OF_TIME

        if val == prev_val:
            n_days, d = days_since_change(val, df[c], df["date"])
            if n_days >= 0:
                d = str(d)
                d = d[4:6] + "/" + d[6:8]

                if prev_val >= 20 and (is_check_field_set or phase in ["publish", "update"]):
                    log.error(row.state, f"{c} value ({val:,}) has not changed since {d} ({n_days} days)")
                else:
                    log.warning(row.state, f"{c} value ({val:,}) has not changed since {d} ({n_days} days)")
            else:
                log.error(row.state, f"{c} value ({val:,}) constant for all time")
            continue

        p_observed = 100.0 * val / prev_val - 100.0

        #TODO: estimate expected increase from recent history
        p_min, p_max = EXPECTED_PERCENT_THRESHOLDS[c]
        if p_observed < p_min or p_observed > p_max:
            log.warning(row.state, f"{c} value ({val:,}) is a {p_observed:.0f}% increase, expected: {p_min:.0f} to {p_max:.0f}%")


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

            log.error(state, f"{col} values decreased from the previous day (on {error_dates_str})")

# ----------------------------------------------------------------

FIT_THRESHOLDS = [0.95, 1.1]

def expected_positive_increase( current: pd.DataFrame, history: pd.DataFrame,
                                log: ResultLog, context: str, config: ForecastConfig=None):
    """
    Fit state-level daily positives data to an exponential and a linear curve.
    Get expected vs actual case increase to determine if current positives
    are within the expected ranges.

    The exponential is used as the upper bound. The linear is used as the lower bound.

    TODO: Eventually these curves will NOT be exp (perhaps logistic?)
          Useful to know which curves have been "leveled" but from a
          data quality perspective, this check would become annoying
    """

    if not config: config = ForecastConfig()

    forecast_date = current.lastUpdateEt.to_pydatetime().strftime('%Y%m%d')
    history = history.loc[history["date"].astype(str) != forecast_date]

    forecast = Forecast()
    forecast.fit(history)
    forecast.project(current)

    if config.plot_models:
        plot_to_file(forecast, f"{config.images_dir}/{context}", FIT_THRESHOLDS)

    state = forecast.state
    date = forecast.date
    actual_value, expected_linear, expected_exp = forecast.results

    min_value = int(FIT_THRESHOLDS[0] * expected_linear)
    max_value = int(FIT_THRESHOLDS[1] * expected_exp)

    if not (min_value <= actual_value <=  max_value):
        direction = "increase"
        if actual_value < expected_linear:
            direction = "drop"

        log.error(state, f"unexpected {direction} in positive cases ({actual_value:,}) for {date}, expected between {min_value:,} and {max_value:,}")



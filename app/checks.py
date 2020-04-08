#
# Individual Check Routines
#
#   Each routine checks a specific aspect of a single state
#
#   If any issues are found, the check routine calls the log to report it.
#   Each message has a category.  See ResultLog for a list of categories.
##
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

from app.util import udatetime

from .qc_config import QCConfig
from .log.result_log import ResultLog
from .modeling.forecast import Forecast
from .modeling.forecast_plot import plot_to_file
from .modeling.forecast_io import save_forecast_hd5, load_forecast_hd5

START_OF_TIME = udatetime.naivedatetime_as_eastern(datetime(2020,1,2))

def missing_tests(log: ResultLog):

    log.internal("Missing", "pending is not testable")
    log.internal("Missing", "recovered should be less than positives")
    
    
    log.internal("Apple/Oranges", "hospitalized and hospitalizedCumulative are different types of measurements")
    log.internal("Apple/Oranges", "icu and icuCumulative are different types of measurements")
    log.internal("Apple/Oranges", "ventilator and ventilatorCumulative  are different types of measurements")

    #log.internal("Missing", "hospitalizedCumulative should always increase")
    #log.internal("Missing", "icuCumulative should always increase")
    #log.internal("Missing", "ventilatorCumulative should always increase")


# ----------------------------------------------------------------

def total(row, log: ResultLog):
    """Check that pendings, positive, and negative sum to the reported total"""

    n_pos, n_neg, n_pending, n_death, n_tot = \
        row.positive, row.negative, row.pending, row.death, row.total

    def bad_value_msg(name: str, val: int) -> str:
        if val == -1000: return f"{name} is blank"
        if val == -1001: return f"{name} is invalid"
        return f"{name} is negative ({val})"

    if n_pending == -1000: # allow blanks
        n_pending = 0

    is_bad = False
    if n_pos < 0:
        is_bad = True
        log.data_entry(row.state, bad_value_msg("positive", n_pos))
    if n_neg < 0:
        is_bad = True
        log.data_entry(row.state, bad_value_msg("negative", n_neg))
    if n_pending < 0:
        is_bad = True
        log.data_entry(row.state, bad_value_msg("pending", n_pending))
    if n_death < 0:
        is_bad = True
        log.data_entry(row.state, bad_value_msg("death", n_death))
    
    n_diff = n_tot - (n_pos + n_neg + n_pending)
    if not is_bad:
        if n_tot < 0:
            log.data_entry(row.state, bad_value_msg("total", n_tot))
        elif n_diff != 0:
            log.data_entry(row.state, f"Formula broken -> Positive ({n_pos}) + Negative ({n_neg}) + Pending ({n_pending}) != Total ({n_tot}), delta = {n_diff}")

def total_tests(row, log: ResultLog):
    """Check that positive, and negative sum to the reported totalTest"""

    # note -- I don't know where this field is in the sheet so this test is not used right now - Josh

    n_pos, n_neg, n_tests = \
        row.positive, row.negative, row.totalTestResults

    n_diff = n_tests - (n_pos + n_neg)
    if n_diff != 0:
        log.data_entry(row.state, f"Formula broken -> Positive ({n_pos}) + Negative ({n_neg}) != Total Tests ({n_tests}), delta = {n_diff}")


def last_update(row, log: ResultLog, config: QCConfig):
    """Source has updated within a reasonable timeframe"""

    updated_at = row.lastUpdateEt.to_pydatetime()
    target_time = row.targetDateEt.to_pydatetime()
    delta = target_time - updated_at
    days = delta.total_seconds() / (24 * 60.0 * 60)

    if days >= 2.0:
        log.data_source(row.state, f"source hasn't updated in {days:.0f} days")
    #elif hours > 18.0:
    #   log.data_source(row.state, f"Last Updated (col T) hasn't been updated in {hours:.0f}  hours")

def last_checked(row, log: ResultLog, config: QCConfig):
    """Data was checked within a reasonable timeframe"""

    if not config.is_near_release: return

    target_date = row.targetDateEt.to_pydatetime()
    updated_at = row.lastUpdateEt.to_pydatetime()
    checked_at = row.lastCheckEt.to_pydatetime()

    delta = updated_at - checked_at
    hours = delta.total_seconds() / (60.0 * 60)
    if hours > 1.0:
        if hours > 2000:
            log.data_entry(row.state, f"Last Check ET (column AJ) is blank")
        else:
            s_updated = updated_at.strftime('%m/%d %H:%M')
            s_checked = checked_at.strftime('%m/%d %H:%M')        
            log.data_entry(row.state, f"Last Check ET (column AJ) is {s_checked} which is less than Last Update ET (column AI)  {s_updated} by {hours:.0f} hours")
        return

    delta = target_date - checked_at
    hours = delta.total_seconds() / (60.0 * 60)
    if hours > 6.0:
        s_checked = checked_at.strftime('%m/%d %H:%M')
        log.data_entry(row.state, f"Last Check ET (column AJ) has not been updated in {hours:.0f} hours ({s_checked} by {row.checker})")
        return


def checkers_initials(row, log: ResultLog, config: QCConfig):
    """Confirm that checker initials are records"""

    target_date = row.targetDateEt.to_pydatetime()
    checked_at = row.lastCheckEt.to_pydatetime()
    if checked_at <= START_OF_TIME: return

    checker = row.checker.strip()
    doubleChecker = row.doubleChecker.strip()

    delta_hours = (target_date - checked_at).total_seconds() / (60.0 * 60.0)

    if checker == "":
        if 0 < delta_hours < 5:
            s_checked = checked_at.strftime('%m/%d %H:%M')
            log.data_entry(row.state, f"missing checker initials (column AK) but checked date set recently (at {s_checked})")
        elif config.is_near_release:
            log.data_entry(row.state, f"missing checker initials (column AK)")
        return
    if doubleChecker == "":
        if config.is_near_release:
            log.data_entry(row.state, f"missing double-checker initials (column AL)")
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
    "positive-small": (.5, 1.5),
    "positive-large": (.75, 1.25),
    "death-small": (.5, 1.5),
    "death-large": (.75, 1.25),
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

    # use median
    xindex = (df.shape[0] // 2)
    # use biggest value
    #xindex = df.shape[0] - 1

    if row.positive > 1000:
        df.sort_values(by="cases", inplace=True)
        mid = df.iloc[xindex]
        if not mid.c_ok:
            logger.warning(f"  {row.state}: positive ({row.positive:,}) does not match county aggregate ({mid.c_min:,} to {mid.c_max:,})")
            log.data_quality(row.state, f"positive ({row.positive:,}) does not match {mid.source} county aggregate ({mid.cases:,}, allow {mid.c_min:,} to {mid.c_max:,})")

    if row.death > 200:
        df.sort_values(by="deaths", inplace=True)
        mid = df.iloc[xindex]
        if not mid.d_ok:
            logger.warning(f"  {row.state}:   death ({row.death:,}) does not match county aggregate ({mid.d_min:,} to {mid.d_max:,})")
            log.data_quality(row.state, f"death ({row.death:,}) does not match {mid.source} county aggregate ({mid.deaths:,}, allow {mid.d_min:,} to {mid.d_max:,})")

# ----------------------------------------------------------------

IGNORE_THRESHOLDS = {
    "positive": 100,
    "negative": 900,
    "death": 20,
}

def find_last_change(val, vec_vals: pd.Series, vec_date) -> Tuple[int, datetime]:
    vals = vec_vals.values
    for i in range(len(vals)):
        if vals[i] != val: 
            sdate = str(vec_date.values[i])
            d = datetime(int(sdate[0:4]), int(sdate[4:6]), int(sdate[6:8]))
            return vals[i], udatetime.naivedatetime_as_eastern(d)
    return 0, None

def consistent_with_history(row, df: pd.DataFrame, log: ResultLog) -> bool:
    """Check that row values match same date in history
    """

    df = df[df.date == row.targetDate]

    #dict_row = row._asdict()

    print(row)
    print(df)
    exit(-1)


def increasing_values(row, df: pd.DataFrame, log: ResultLog, config: QCConfig = None) -> bool:
    """Check that new values more than previous values

    df contains the historical values (newest first).  offset controls how many days to look back.
    consolidate lines if everything changed

    return False if it looks like we have no new data for this source so we can bypass other tests
    """

    if not config: config = QCConfig()

    df = df[df.date < row.targetDate]

    dict_row = row._asdict()

    # local time is an editable field that it supposed to be the last time the data changed.
    # last_updated is the same value but adjusted to eastern TZ 
    if "localTime" in dict_row:
        local_time = row.localTime 
        d_local = local_time.year * 10000 + local_time.month * 100 + local_time.day
    else:
        local_time = None
        d_local = 0

    last_updated = row.lastUpdateEt
    d_updated = last_updated.year * 10000 + last_updated.month * 100 + last_updated.day

    # target date of run
    s_target = str(row.targetDate)
    d_target = datetime(int(s_target[0:4]), int(s_target[4:6]), int(s_target[6:8]))
    d_target = udatetime.naivedatetime_as_eastern(d_target)

    d_last_change = udatetime.naivedatetime_as_eastern(datetime(2020,1,1))

    debug = config.enable_debug

    if debug: logger.debug(f"check {row.state}")

    fieldList = ["positive", "negative", "death", "hospitalizedCumulative", "inIcuCumulative", "onVentilatorCumulative"]
    displayList = ["positive", "negative", "death", "hospitalized", "icu", "ventilator"]

    source_messages = []
    has_issues, consolidate, n_days, n_days_prev = False, True, -1, 0
    for c in fieldList:
        val = dict_row.get(c)
        if val is None:
            log.internal(row.state, f"{c} missing column")
            has_issues, consolidate = True, False
            if debug: logger.debug(f"  {c} missing column")
            continue
        if not c in df.columns:
            log.internal(row.state, f"{c} missing history column")
            has_issues, consolidate = True, False
            if debug: logger.debug(f"  {c} missing history column")
            continue

        vec = df[c].values

        prev_val = vec[0] if vec.size > 0 else 0
        prev_date = df["date"].iloc[0] if vec.size > 0 else 0


        if val < prev_val and (val > 0 and prev_val != 0): # negative values indicate blank/errors
            sd = str(prev_date)[4:] if prev_date > 0 else "-"
            sd = sd[0:2] + "/" + sd[2:4] 
            log.data_quality(row.state, f"{c} ({val:,}) decreased from {prev_val:,} as-of {sd}")
            has_issues, consolidate = True, False
            if debug: logger.debug(f"  {c} ({val:,}) decreased from {prev_val:,} as-of {sd}")
            continue

        # allow value to be the same if below a threshold, default to 10
        t = IGNORE_THRESHOLDS.get(c)
        if t == None: t = 10
        if val < t: 
            if debug: logger.debug(f"  {c} ({val:,}) is below threshold -> ignore 'same' check")
            continue

        if val == -1000:
            log.data_entry(row.state, f"{c} value cannot be converted to a number")
            has_issues, consolidate = True, False
            if debug: logger.debug(f"  {c} was not a number in source data")
            continue

        if val == prev_val:
            changed_val, changed_date = find_last_change(val, df[c], df["date"])

            n_days = int((d_target - changed_date).total_seconds() // (60*60*24))

            # ignore 2-day stale if not near release
            if not config.is_near_release and n_days < 3:
                continue

            if n_days >= 0:
                d_last_change = max(d_last_change, changed_date)

                source_messages.append(f"{c} ({val:,}) hasn't changed since {changed_date.month}/{changed_date.day} ({n_days} days)")
                
                # check if we can still consolidate results
                if n_days_prev == 0:
                    n_days_prev = n_days
                    if debug: logger.debug(f"  {c} ({val:,}) hasn't changed since {changed_date.month}/{changed_date.day} ({n_days} days)")
                elif n_days_prev == n_days:
                    if debug: logger.debug(f"  {c} ({val:,}) also hasn't changed since {changed_date.month}/{changed_date.day}")
                else:
                    consolidate = False
                    if debug: logger.debug(f"  {c} ({val:,}) hasn't changed since {changed_date.month}/{changed_date.day} ({n_days} days ago) -> force individual lines ")
            else:
                d_last_change = max(d_last_change, df["date"].values[-1])
                has_issues, consolidate = True, False
                log.data_source(row.state, f"{c} ({val:,}) constant for all time")
                if debug: logger.debug(f"  {c} ({val:,}) constant -> force individual lines ")
        else:
            consolidate = False
            if debug: logger.debug(f"  {c} ({val:,}) changed from {prev_val:,} on {prev_date}")


    if len(source_messages) == 0:
        if debug: logger.debug(f"  no source messages -> has_issues={has_issues}")
        return has_issues

    # alert if local time appears to updated incorrectly
    if d_local != 0 and d_local != d_updated:
        sd = str(d_last_change)
        sd = sd[4:6] + "/" + sd[6:8]
        sd_local = f"{local_time.month}/{local_time.day} {local_time.hour:02}:{local_time.minute:02}"
        checker = row.checker
        if checker == "": checker = "??"
        log.data_entry(row.state, f"checker {checker} set local time (column V) to {sd_local} but values haven't changed since {sd} ({n_days:.0f} days ago)")
        #has_issues = True
        if debug: logger.debug(f"  checker {checker} set local time (column V) to {sd_local} but values haven't changed since {sd} ({n_days:.0f} days ago)")

    if consolidate:
        names = "/".join(displayList)
        if config.is_near_release or n_days >= 3.0:
            log.data_source(row.state, f"cumulative values ({names}) haven't changed since {d_last_change.month}/{d_last_change.day} ({n_days:.0f} days)")
        if debug: logger.debug(f"  cumulative values ({names}) haven't changed since {d_last_change.month}/{d_last_change.day} ({n_days:.0f} days)")
    else:
        for m in source_messages: log.data_source(row.state, m)
        if debug: logger.debug(f"  {row.state}: record {len(source_messages)} source issue(s) to log")
    return has_issues

# disabled because the fields measure different things. apples-to-oranges

# def delta_vs_cumulative(row, df: pd.DataFrame, log: ResultLog, config: QCConfig = None):
#     """Checks that cumulative = delta + previous day 
#     """
#
#     if not config: config = QCConfig()
#
#     df = df[df.date < row.targetDate]
#
#     dict_row = row._asdict()
#
#     fieldList = ["hospitalized", "inIcu", "onVentilator"]
#
#     debug = False
#
#     for c in fieldList:
#         c2 = c + "Cumulative"
#         val = dict_row.get(c)
#         if val is None:
#             logger.error(f"  {row.state}: {c} is missing")
#             log.internal(row.state, f"{c} missing column")
#             continue
#         cuml_val = dict_row.get(c2)
#         if cuml_val is None:
#             logger.error(f"  {row.state}: {c2} is missing")
#             log.internal(row.state, f"{c2} missing column")
#             continue
#         if not c2 in df.columns:
#             logger.error(f"  {row.state}: {c2} is missing from history")
#             log.internal(row.state, f"{c2} missing history column")
#             continue
#
#         vec = df[c2].values
#         prev_cuml_val = vec[0] if vec.size > 0 else 0
#
#         prev_date = df["date"].iloc[0] if vec.size > 0 else 0
#         sd = str(prev_date)[4:] if prev_date > 0 else "-"
#         sd = sd[0:2] + "/" + sd[2:4] 
#
#         if debug: logger.debug(f"  test {c}")
#
#         if cuml_val < 0:
#             if cuml_val == -1000:
#                 if debug: logger.debug(f"    {row.state}: {c2} is blank -> treat as zero")
#                 cuml_val = 0
#             else:
#                 if debug: logger.debug(f"    {row.state}: {c2} is invalid -> skip")
#                 continue
#
#         if val < 0:
#             if val == -1000:
#                 if cuml_val != prev_cuml_val:
#                     # disabled for now at request of SJ Klein
#                     # log.data_entry(row.state, f"{c} is blank but {c2} current = {cuml_val:,} and prev = {prev_cuml_val:,} on {sd}")
#                     continue
#                 else:
#                     if debug: logger.debug(f"    {row.state}: {c} is blank -> treat as zero ")
#                 val = 0
#             else:
#                 if debug: logger.debug(f"    {row.state}: {c} is invalid")
#                 log.data_entry(row.state, f"{c} is invalid")
#                 continue
#        
#         if prev_cuml_val + val != cuml_val:
#             if debug: logger.warning(f"    {row.state}: {prev_cuml_val} + {val} != {cuml_val}")
#             if prev_cuml_val == cuml_val:
#                 log.data_entry(row.state, f"{c2} {cuml_val:,} has not been updated, {c} = {val:,} and prev = {prev_cuml_val:,} on {sd}")
#             else:
#                 log.data_quality(row.state, f"{c2} {cuml_val:,} != {c} {val:,} + prev {prev_cuml_val:,} on {sd}")
#         else:
#             if debug: logger.debug(f"    {row.state}: {prev_cuml_val} + {val} == {cuml_val}")


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

    # limit to N >= 300
    if actual_value < 300: return

    # --- sanity checks ----
    debug = config.enable_debug
    if debug: logger.debug(f"{forecast.state}: actual = {actual_value:,}, linear={expected_linear:,}, exp={expected_exp:,}")

    is_bad = False
    if 100 < expected_linear > 100_000:
        logger.error(f"{forecast.state}: actual = {actual_value:,}, linear model = {expected_linear:,} ")
        log.internal(forecast.state, f"actual = {actual_value:,}, linear model = {expected_linear:,} ")
        is_bad = True
    if 100 < expected_linear > 100_000:
        logger.error(f"{forecast.state}: actual = {actual_value:,}, exponental model = {expected_exp:,} ")
        log.internal(forecast.state, f"actual = {actual_value:,}, exponental model = {expected_exp:,} ")
        is_bad = True
    if (not is_bad) and (expected_linear >= expected_exp):
        logger.error(f"{forecast.state}: actual = {actual_value:,}, linear model ({expected_linear:,}) > exponental model ({expected_exp:,})")
        log.internal(forecast.state, f"actual = {actual_value:,}, linear model ({expected_linear:,}) > exponental model ({expected_exp:,})")
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

#
# Individual Check Routines
#
#   Each routine checks a specific aspect of a single state
#
#   If any issues are found, the check routine calls the log to report it.
#   The three log methods are info/warning/error.
#

from datetime import datetime
import pandas as pd
import numpy as np
from scipy.optimize import curve_fit
import matplotlib
import matplotlib.pyplot as plt
import warnings

import udatetime
from result_log import ResultLog

def _format_date(date:str) -> str:
    """return YYYYmmdd as YYYY-mm-dd"""
    return f"{date[:4]}-{date[4:6]}-{date[6:]}"

def total(row, log: ResultLog):
    """Check that pendings, positive, and negative sum to the reported total"""

    n_pos, n_neg, n_pending, n_tot = \
        row.positive, row.negative, row.pending, row.total

    n_diff = n_tot - (n_pos + n_neg + n_pending)
    if n_diff != 0:
        log.error(row.state, f"Formula broken -> Postive ({n_pos}) + Negative ({n_neg}) + Pending ({n_pending}) != Total ({n_tot}), delta = {n_diff}")


def last_update(row, log: ResultLog):
    """Check that the data has been updated within a reasonable timeframe"""

    current_time = udatetime.now_as_eastern()
    updated_at = row.lastUpdateEt.to_pydatetime()
    delta = current_time - updated_at
    hours = delta.total_seconds() / (60.0 * 60)

    if hours > 36.0:
        log.error(row.state, f"Last Updated (col T) hasn't been updated in {hours:.0f}  hours")
    #elif hours > 18.0:
    #   log.error(row.state, f"Last Updated (col T) hasn't been updated in {hours:.0f}  hours")


def positives_rate(row, log: ResultLog):
    """Check that positives compose <20% test results"""

    n_pos, n_neg, n_deaths = row.positive, row.negative, row.death
    n_tot = n_pos + n_neg + n_deaths

    percent_pos = 100.0 * n_pos / n_tot if n_tot > 0 else 0.0
    if n_tot > 100:
        if percent_pos > 20.0:
            log.error(row.state, f"Too many positive {percent_pos:.0f}% (positive={n_pos:,}, total={n_tot:,})")
    else:
        if percent_pos > 50.0:
            log.error(row.state, f"Too many positive {percent_pos:.0f}% (positive={n_pos:,}, total={n_tot:,})")

def death_rate(row, log: ResultLog):
    """Check that deaths are <2% of test results"""

    n_pos, n_neg, n_deaths = row.positive, row.negative, row.death
    n_tot = n_pos + n_neg + n_deaths

    percent_deaths = 100.0 * n_deaths / n_tot if n_tot > 0 else 0.0
    if n_tot > 100:
        if percent_deaths > 2.0:
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

    n_pos, n_neg, n_pending, n_deaths = row.positive, row.negative, row.pending, row.death
    n_tot = n_pos + n_neg + n_deaths
    percent_pending = 100.0 * n_pending / n_tot if n_tot > 0 else 0.0

    if n_tot > 1000:
        if percent_pending > 20.0:
            log.warning(row.state, f"Too many pending {percent_pending:.0f}% (pending={n_pending:,}, total={n_tot:,})")
    else:
        if percent_pending > 80.0:
            log.warning(row.state, f"Too many pending {percent_pending:.0f}% (pending={n_pending:,}, total={n_tot:,})")

IGNORE_THRESHOLDS = {
    "positive": 20,
    "negative": 80,
    "death": 5,
    "total": 100
}
EXPECTED_PERCENT_THRESHOLDS = {
    "positive": (5,30),
    "negative": (5,40),
    "death": (0,5),
    "total": (5,40)
}

def increasing_values(row, df: pd.DataFrame, log: ResultLog, offset=0):
    """Check that new values more than previous values

    df contains the historical values (newest first).  offset controls how many days to look back.
    """

    dict_row = row._asdict()

    for c in ["positive", "negative", "death", "total"]:
        val = dict_row[c]
        prev_val = df[c].values[offset]

        if val < prev_val:
            log.error(row.state, f"{c} value ({val:,}) is less than prior value ({prev_val:,})")

        # allow value to be the same if below a threshold
        if val < IGNORE_THRESHOLDS[c]: continue

        if val == prev_val:
            log.error(row.state, f"{c} value ({val:,}) is same as prior value ({prev_val:,})")
            continue

        p_observed = 100.0 * val / prev_val - 100.0

        #TODO: estimate expected increase from recent history
        p_min, p_max = EXPECTED_PERCENT_THRESHOLDS[c]
        if p_observed < p_min or p_observed > p_max:
            log.warning(row.state, f"{c} value ({val:,}) is a {p_observed:.1f}% increase, expected: {p_min:.1f} to {p_max:.1f}%")


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

def _exp_curve(x, a, b):
    return a * np.exp(b * x)

def _linear_fit(x, m, b):
    return m*x + b

def _get_distribution_fit(x: pd.Series, y: pd.Series, dist_func):

    np.random.seed(1729)

    x = np.array(x.values, dtype=float)
    y = np.array(y.values, dtype=float)

    popt, pcov = curve_fit(dist_func, x, y, p0=(4, 0.1))
    return popt

def expected_increase(df: pd.DataFrame, log:ResultLog):
    """
    Fit state-level timeseries data to an exponential curve to
    Get expected vs actual case increase

    TODO: Eventually these curves will NOT be exp (perhaps logistic?)
          Useful to know which curves have been "leveled" but from a
          data quality persepctive, this check would become annoying
    """
    warnings.filterwarnings('ignore')

    cases_df = (df
        .sort_values("date", ascending=True)
        .reset_index(drop=True)
        .rename_axis('index')
        .reset_index())

    to_fit_exp = cases_df[:-1]
    to_fit_linear = cases_df[-5:-1]
    to_forecast = cases_df.tail(1)

    fitted_linear = _get_distribution_fit(to_fit_linear["index"], to_fit_linear["positive"], _linear_fit)
    fitted_exp = _get_distribution_fit(to_fit_exp["index"], to_fit_exp["positive"], _exp_curve)

    state = df["state"].values[0]
    date = _format_date(to_forecast["date"].values[0].astype(str))
    actual_value = to_forecast["positive"].values[0]
    expected_exp = _exp_curve(to_forecast["index"].values[0], *fitted_exp).round().astype(int)
    expected_linear = _linear_fit(to_forecast["index"].values[0], *fitted_linear).round().astype(int)

    # Plot case growth (expected and actuals)
    matplotlib.style.use('ggplot')

    plt.figure(figsize=(9,15))
    ax = cases_df.plot.bar(x="index", y="positive", color="gray", alpha=.7, label="actual positives growth")
    plt.plot(cases_df["index"], _exp_curve(cases_df["index"], *fitted_exp), color="red", label="exponential fit")
    plt.plot(cases_df["index"], _linear_fit(cases_df["index"], *fitted_linear), color="black", label="projected growth")

    plt.title(f"{state} ({date}): Expected {expected_linear}, got {actual_value}")
    ax.set_xticklabels(cases_df["date"].apply(lambda d: _format_date(str(d))), rotation=90)
    plt.xlabel("Day")
    plt.ylabel("Number of positive cases")
    plt.ylim(0,max(cases_df["positive"].max(), expected_exp, expected_linear)+10)
    plt.legend()

    # TODO: Might want to save these to s3?
    # This write-to-file step adds ~1 sec of runtime / state
    plt.savefig(f"./images/predicted_positives_{state}_{date}.png", dpi=250, bbox_inches = "tight")

    # Log errors and warning
    if not (expected_linear*.95 <= actual_value <=  expected_exp*1.1):
        direction = "increase"
        if actual_value < expected_linear:
            direction = "drop"

        log.error(state, f"unexpected {direction} in positive cases (expected between {expected_linear} and {expected_exp}, got {actual_value} for {date}))")



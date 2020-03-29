#
# Individual Check Routines
#
#   Each routine checks a specific aspect of a single state
#
#   If any issues are found, the check routine calls the log to report it.
#   The three log methods are info/warning/error.
#

#import pdb
import pandas as pd
import udatetime
from datetime import datetime

from result_log import ResultLog


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
        raise Exception("Expeted input to be for a single state")

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

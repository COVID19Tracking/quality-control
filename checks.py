import pandas as pd
import pdb

import udatetime
from datetime import datetime

def total(row) -> (str, str):
    """Check that pendings, positive, and negative sum to the reported total"""

    error = None
    warning = None

    n_pos, n_neg, n_pending, n_tot = \
        row.positive, row.negative, row.pending, row.total

    n_diff = n_tot - (n_pos + n_neg + n_pending)
    if n_diff != 0:
        error = f"Formula broken -> Postive ({n_pos}) + Negative ({n_neg}) + Pending ({n_pending}) != Total ({n_tot}), delta = {n_diff}"
    return (error, warning)

def last_update(row) -> (str, str):
    """Check that the data has been updated within a reasonable timeframe"""
    error = None
    warning = None

    current_time = udatetime.now_as_eastern()
    updated_at = udatetime.naivedate_as_eastern(row.lastUpdateEt.to_pydatetime())
    delta = current_time - updated_at
    hours = delta.total_seconds() / (60.0 * 60)

    if hours > 36.0:
        error = f"Last Updated (col T) hasn't been updated in {hours:.0f}  hours"
    #elif hours > 18.0:
    #    warning = f"{state} hasn't been updated in {hours:.0f} hours"

    return (error, warning)

def positives_rate(row) -> (str, str):
    """Check that positives compose <20% test results"""

    error = None
    warning = None

    n_pos, n_neg, n_deaths = row.positive, row.negative, row.death
    n_tot = n_pos + n_neg + n_deaths

    percent_pos = 100.0 * n_pos / n_tot if n_tot > 0 else 0.0
    if n_tot > 100:
        if percent_pos > 20.0:
            error = f"{row.state} has too many positive {percent_pos:.0f}% (positive={n_pos:,}, total={n_tot:,})"
    else:
        if percent_pos > 50.0:
            error = f"{row.state} has too many positive {percent_pos:.0f}% (positive={n_pos:,}, total={n_tot:,})"

    return (error, warning)

def death_rate(row) -> (str, str):
    """Check that deaths are <2% of test results"""

    error = None
    warning = None

    n_pos, n_neg, n_deaths = row.positive, row.negative, row.death
    n_tot = n_pos + n_neg + n_deaths

    percent_deaths = 100.0 * n_deaths / n_tot if n_tot > 0 else 0.0
    if n_tot > 100:
        if percent_deaths > 2.0:
            error = f"{row.state} has too many deaths {percent_deaths:.0f}% (positive={n_deaths:,}, total={n_tot:,})"
    else:
        if percent_deaths > 10.0:
            error = f"{row.state} has too many deaths {percent_deaths:.0f}% (positive={n_deaths:,}, total={n_tot:,})"

    return (error, warning)

def less_recovered_than_positive(row) -> (str, str):
    """Check that we don't have more recovered than positive"""

    error = None
    warning = None

    if row.recovered > row.positive:
        error = f"{row.state} has more recovered than positive (recovered={row.recovered:,}, positive={row.positive:,})"

    return (error, warning)

def pendings_rate(row) -> (str, str):
    """Check that pendings are not more than 20% of total"""

    error = None
    warning = None

    n_pos, n_neg, n_pending, n_deaths = row.positive, row.negative, row.pending, row.death
    n_tot = n_pos + n_neg + n_deaths
    percent_pending = 100.0 * n_pending / n_tot if n_tot > 0 else 0.0

    if n_tot > 1000:
        if percent_pending > 20.0:
            warning = f"{row.state} has too many pending {percent_pending:.0f}% (pending={n_pending:,}, total={n_tot:,})"
    else:
        if percent_pending > 80.0:
            warning = f"{row.state} has too many pending {percent_pending:.0f}% (pending={n_pending:,}, total={n_tot:,})"

    return (error, warning)

def monotonically_increasing(df) -> (str, str):
    """Check that timeseries values are monotonically increasing"""

    error = None
    warning = None

    columns_to_check = ["positive", "negative","hospitalized", "death"]
    state = df["state"].values[0]

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

            if error is None:
                error = f"{state} has {col} values decreased from the previous day (on {error_dates_str})"
            else:
                error = f"{error}\n{state} has {col} values decreased from the previous day (on {error_dates_str})"

    return (error, warning)

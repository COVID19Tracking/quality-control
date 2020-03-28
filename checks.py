from loguru import logger
import pandas as pd
import numpy as np

import udatetime
from datetime import datetime

def last_update(row) -> (str, str):
    """Check that the data has been updated within a reasonable timeframe"""
    error = None
    warning = None

    current_time = udatetime.now_as_eastern()
    updated_at = udatetime.naivedate_as_eastern(row.lastUpdateEt.to_pydatetime())
    delta = current_time - updated_at
    hours = delta.total_seconds() / (60.0 * 60)

    if hours > 36.0:
        error = f"{row.state} hasn't been updated in {hours:.0f}  hours"
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

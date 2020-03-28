#
# The main check loop
# 
from loguru import logger
#from typing import Tuple
import pandas as pd
import numpy as np

import udatetime
from sources import GoogleWorksheet
from datetime import datetime

def check_all():

    gs = GoogleWorksheet()

    # get working data from https://docs.google.com/spreadsheets/d/1MvvbHfnjF67GnYUDJJiNYUmGco5KQ9PW0ZRnEP9ndlU/edit#gid=1777138528
    df = gs.load_dev_from_google()

    current_time = udatetime.now_as_eastern()

    logger.info("==| Internal Consistency Check |==============================================================")
    for row in df.itertuples():
        check_one(row, current_time)
    logger.info("")

    logger.info("==| Previous Day Check |==============================================================")

    # get published data from https://covidtracking.com/api/states/daily
    #   this includes all previous dates
    #df_published = gs.load_published_from_api()



def check_one(row, current_time: datetime) -> bool:

    state = row.State
    #logger.info(f"check {state}")
    # print(f"values = {row})

    n_pos, n_neg, n_pending, n_recovered, n_deaths = \
        row.Positive, row.Negative, row.Pending, row.Recovered, row.Deaths
    n_tot = n_pos + n_neg + n_deaths

    errors = []
    warnings = []

    # ===========================================
    # check last update
    updated_at = udatetime.naivedate_as_eastern(row.Last_Update.to_pydatetime())
    
    delta = current_time - updated_at
    hours = delta.total_seconds() / (60.0 * 60)
    if hours > 36.0:
        errors.append(f"{state} hasn't been updated in {hours:.0f}  hours")
    #elif hours > 18.0:
    #    warnings.append(f"{state} hasn't been updated in {hours:.0f} hours")


    # ===========================================
    # simple consistency checks

    # positive should be less than 20% 
    percent_pos = 100.0 * n_pos / n_tot if n_tot > 0 else 0.0
    if n_tot > 100:
        if percent_pos > 20.0:
            errors.append(f"{state} has too many positive {percent_pos:.0f}% (positive={n_pos:,}, total={n_tot:,})")
    else:
        if percent_pos > 50.0:
            errors.append(f"{state} has too many positive {percent_pos:.0f}% (positive={n_pos:,}, total={n_tot:,})")

    # deaths should be less than 2% 
    percent_deaths = 100.0 * n_deaths / n_tot if n_tot > 0 else 0.0
    if n_tot > 100:
        if percent_deaths > 2.0:
            errors.append(f"{state} has too many deaths {percent_deaths:.0f}% (positive={n_deaths:,}, total={n_tot:,})")
    else:
        if percent_deaths > 10.0:
            errors.append(f"{state} has too many deaths {percent_deaths:.0f}% (positive={n_deaths:,}, total={n_tot:,})")

    # can't have more recovered than positive 
    if n_recovered > n_pos:
        errors.append(f"{state} has more recovered than positive (recovered={n_recovered:,}, positive={n_pos:,})")

    # pendings shouldn't be more than 20% of total 
    percent_pending = 100.0 * n_pending / n_tot if n_tot > 0 else 0.0
    if n_tot > 1000:
        if percent_pending > 20.0:
            warnings.append(f"{state} has too many pending {percent_pending:.0f}% (pending={n_pending:,}, total={n_tot:,})")
    else:
        if percent_pending > 80.0:
            warnings.append(f"{state} has too many pending {percent_pending:.0f}% (pending={n_pending:,}, total={n_tot:,})")


    # ===========================================
    # missing data check
    # (note -- this should really check against published results)

    # can't have more recovered than positive 
    if n_recovered > n_pos:
        errors.append(f"{state} has more recovered than positive (recovered={n_recovered:,}, positive={n_pos:,})")



    if len(errors) == 0 and len(warnings) == 0:
        #logger.info(f"check {state} --> OKAY")
        return True

    logger.warning(f"check {state} -->")
    for m in errors:
        logger.error(f"   |  {m}")
    for m in warnings:
        logger.warning(f"   |  {m}")
    return False

if __name__ == "__main__":
    check_all()

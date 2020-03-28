#
# The main check loop
# 
from loguru import logger
from sources import GoogleWorksheet
#from typing import Tuple

import pandas as pd
import numpy as np
import re

def check_all():

    gs = GoogleWorksheet()

    # get data from https://docs.google.com/spreadsheets/d/1MvvbHfnjF67GnYUDJJiNYUmGco5KQ9PW0ZRnEP9ndlU/edit#gid=1777138528
    dev_id = gs.get_sheet_id_by_name("dev")
    df = gs.read_as_frame(dev_id, "Worksheet!G3:R60")
    
    for c in df.columns[1:]: 
        df[c] = df[c].str.strip().replace("", "0").replace(re.compile(","), "")
        df[c] = df[c].astype(np.int)

    for row in df.itertuples():
        check_one(row)

def check_one(row) -> bool:

    state = row.State
    #logger.info(f"check {state}")
    # print(f"values = {row})

    n_pos, n_neg, n_pending, n_recovered, n_deaths = \
        row.Positive, row.Negative, row.Pending, row.Recovered, row.Deaths
    n_tot = n_pos + n_neg + n_deaths

    errors = []
    warnings = []

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

#
# The main check loop
# 
from loguru import logger
from sources import GoogleWorksheet

import pandas as pd
import numpy as np
import re

def check():

    gs = GoogleWorksheet()

    # get data from https://docs.google.com/spreadsheets/d/1MvvbHfnjF67GnYUDJJiNYUmGco5KQ9PW0ZRnEP9ndlU/edit#gid=1777138528
    dev_id = gs.get_sheet_id_by_name("dev")
    df = gs.read_as_frame(dev_id, "Worksheet!G3:R60")
    
    for c in df.columns[1:]: 
        df[c] = df[c].str.strip().replace("", "0").replace(re.compile(","), "")
        df[c] = df[c].astype(np.int)
    print(df)



if __name__ == "__main__":
    check()
